"""
Competitor scraper — shared configuration.
"""

import os

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH         = os.path.join(BASE_DIR, "products.db")
CHECKPOINT_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "checkpoints")

# ── HTTP ──────────────────────────────────────────────────────────────────────
USER_AGENT      = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_DELAY   = 2.0    # seconds between requests
REQUEST_TIMEOUT = 20     # seconds

# ── Category mapping (competitor labels → IKOR canonical) ─────────────────────
CATEGORY_MAP = {
    # Phones / smartphones
    "smartphone":           "Smartphony",
    "smartphones":          "Smartphony",
    "phone":                "Smartphony",
    "mobile phone":         "Smartphony",
    "téléphone portable":   "Smartphony",
    # Laptops
    "laptop":               "Notebooky",
    "laptops":              "Notebooky",
    "notebook":             "Notebooky",
    "ordinateur portable":  "Notebooky",
    # Tablets
    "tablet":               "Tablety",
    "tablets":              "Tablety",
    "tablette":             "Tablety",
    # Washing machines
    "washing machine":      "Pračky",
    "washing machines":     "Pračky",
    "washer":               "Pračky",
    "lave-linge":           "Pračky",
    "lave linge":           "Pračky",
    # Dishwashers
    "dishwasher":           "Myčky",
    "dishwashers":          "Myčky",
    "lave-vaisselle":       "Myčky",
    "lave vaisselle":       "Myčky",
    # Fridges
    "refrigerator":         "Ledničky",
    "refrigerators":        "Ledničky",
    "fridge":               "Ledničky",
    "réfrigérateur":        "Ledničky",
    # TVs
    "television":           "Televize",
    "televisions":          "Televize",
    "tv":                   "Televize",
    "téléviseur":           "Televize",
    # Vacuums
    "vacuum":               "Vysavače",
    "vacuum cleaner":       "Vysavače",
    "vacuums":              "Vysavače",
    "aspirateur":           "Vysavače",
    # Headphones
    "headphones":           "Sluchátka",
    "headphone":            "Sluchátka",
    "earbuds":              "Sluchátka",
    "wireless earbuds":     "Sluchátka",
    "wireless headphones":  "Sluchátka",
    # Coffee machines
    "coffee maker":         "Kávovary",
    "coffee machine":       "Kávovary",
    # Lawn mower
    "tondeuse":             "Zahradní technika",
    "lawn mower":           "Zahradní technika",
    # Pressure washer
    "nettoyeur haute pression": "Zahradní technika",
    "pressure washer":          "Zahradní technika",
}


def canonical_category(source_category: str) -> str:
    """Map a source category label to an IKOR canonical category."""
    if not source_category:
        return "Ostatní"
    return CATEGORY_MAP.get(source_category.strip().lower(), source_category.strip())
