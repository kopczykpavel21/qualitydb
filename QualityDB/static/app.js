/* ============================
   QualityDB – Frontend Logic
   ============================ */

let currentPage = 1;
let debounceTimer = null;
let isListView = false;
let activeKeyword = "";
let avoidMode = false;
let categoriesTree = [];   // [{main, subs:[{sub,count}]}]
let competitorBrands = new Set();  // lowercase brand names with competitor data

const SOURCE_LABELS = {
  alza: "Alza.cz", heureka: "Heureka.cz", zbozi: "Zbozi.cz",
  amazon: "Amazon.de", amazon_us: "Amazon.com", otto: "Otto.de", warentest: "Stiftung Warentest",
  dtest: "D-test.cz", datart: "Datart.cz", ceneo: "Ceneo.pl",
  heureka_sk: "Heureka.sk", conrad: "Conrad.de"
};

function priceStr(p) {
  if (p.currency === "USD" && p.Price_EUR) return "$" + p.Price_EUR.toLocaleString("en-US", {minimumFractionDigits: 0, maximumFractionDigits: 2});
  if (p.currency === "PLN" && p.Price_CZK) return Math.round(p.Price_CZK).toLocaleString("pl-PL") + " zł";
  if (p.Price_EUR) return Math.round(p.Price_EUR).toLocaleString("de-DE") + " €";
  if (p.Price_CZK) return Math.round(p.Price_CZK).toLocaleString("cs-CZ") + " Kč";
  return "";
}

// ---- Sort dropdown ----
function buildSortOptions() {
  const options = [
    { value: "RecommendRate_pct_desc", label: "Recommend rate (high → low)" },
    { value: "RecommendRate_pct",      label: "Recommend rate (low → high)" },
    { value: "AvgStarRating_desc",     label: "Star rating (high → low)" },
    { value: "AvgStarRating",          label: "Star rating (low → high)" },
    { value: "ReviewsCount_desc",      label: "Most reviewed" },
    { value: "Price_CZK",              label: "Price (low → high)" },
    { value: "Price_CZK_desc",         label: "Price (high → low)" },
    { value: "Price_EUR",              label: "Price USD (low → high)" },
    { value: "Price_EUR_desc",         label: "Price USD (high → low)" },
    { value: "ReturnRate_pct",         label: "Return rate (low → high)" },
    { value: "Name",                   label: "Name (A → Z)" },
  ];
  const sel = document.getElementById("sort-by");
  sel.innerHTML = options.map(o =>
    `<option value="${o.value}">${o.label}</option>`
  ).join("");
  sel.value = "RecommendRate_pct_desc";
}

// ---- Quality badge ----
function qualityBadge(p) {
  // For Stiftung Warentest: use grade label from details_json.sub_ratings.overall
  if (p.source === "warentest" && p.details_json) {
    try {
      const dj = typeof p.details_json === "string" ? JSON.parse(p.details_json) : p.details_json;
      const overall = (dj.sub_ratings || {}).overall || {};
      const label = overall.label;
      const grade = overall.grade;
      if (label === "sehr gut")    return `<span class="quality-badge badge-excellent">🏆 Sehr gut</span>`;
      if (label === "gut")         return `<span class="quality-badge badge-good">✅ Gut</span>`;
      if (label === "befriedigend") return `<span class="quality-badge badge-warn">⚠️ Befriedigend</span>`;
      if (label === "ausreichend") return `<span class="quality-badge badge-bad">❌ Ausreichend</span>`;
      if (label === "mangelhaft")  return `<span class="quality-badge badge-bad">❌ Mangelhaft</span>`;
      // Fallback: use grade number if label missing
      if (grade !== undefined) {
        if (grade <= 1.5) return `<span class="quality-badge badge-excellent">🏆 Sehr gut</span>`;
        if (grade <= 2.5) return `<span class="quality-badge badge-good">✅ Gut</span>`;
        if (grade <= 3.5) return `<span class="quality-badge badge-warn">⚠️ Befriedigend</span>`;
        return `<span class="quality-badge badge-bad">❌ Ausreichend</span>`;
      }
    } catch(e) {}
  }
  // For D-test: use overall_score from details_json
  let score = null;
  if (p.source === "dtest" && p.details_json) {
    try {
      const dj = typeof p.details_json === "string" ? JSON.parse(p.details_json) : p.details_json;
      score = dj.overall_score;
    } catch(e) {}
  }
  // Fallback to RecommendRate_pct for CZ/DE/PL sources
  const rec = p.RecommendRate_pct;

  if (score !== null && score !== undefined) {
    if (score >= 80) return `<span class="quality-badge badge-excellent">🏆 D-test Top</span>`;
    if (score >= 70) return `<span class="quality-badge badge-good">✅ D-test Dobrý</span>`;
    return "";
  }
  if (rec !== null && rec !== undefined) {
    if (rec >= 97) return `<span class="quality-badge badge-excellent">🏆 Top Pick</span>`;
    if (rec >= 93) return `<span class="quality-badge badge-excellent">⭐ Excellent</span>`;
    if (rec >= 88) return `<span class="quality-badge badge-good">✅ Good</span>`;
  }
  // For Amazon US: use star rating + review count as quality signal
  if (p.source === "amazon_us") {
    const stars = p.AvgStarRating;
    const reviews = p.ReviewsCount || 0;
    if (stars >= 4.7 && reviews >= 1000) return `<span class="quality-badge badge-excellent">🏆 Top Rated</span>`;
    if (stars >= 4.5 && reviews >= 500)  return `<span class="quality-badge badge-good">⭐ Highly Rated</span>`;
  }
  return "";
}

const COMPETITOR_SOURCE_META = {
  french_index: { label: "🇫🇷 EU Index",    color: "#1d4ed8", desc: "French Repairability Index (0–100)" },
  ifixit:       { label: "🔧 iFixit",        color: "#e05b2a", desc: "iFixit Repairability Score (0–100)" },
  yale:         { label: "🏠 Yale",          color: "#059669", desc: "Yale Appliance reliability score (0–100)" },
  bifl:         { label: "♾️ BIFL",          color: "#7c3aed", desc: "Buy It For Life durability (0–100)" },
  openrepair:   { label: "🔨 Fix Rate",      color: "#0891b2", desc: "Community repair fix rate (0–100)" },
};

// ---- State ----
function getFilters() {
  const sortVal = document.getElementById("sort-by").value;
  const [sortField, sortDir] = sortVal.endsWith("_desc")
    ? [sortVal.replace("_desc", ""), "desc"]
    : [sortVal, "asc"];

  const starsVal = parseFloat(document.getElementById("filter-stars").value);
  const returnVal = parseFloat(document.getElementById("filter-return").value);
  const reviewsVal = parseInt(document.getElementById("filter-reviews").value);
  const recommendVal = parseInt(document.getElementById("filter-recommend").value);

  return {
    q: document.getElementById("search-input").value.trim(),
    main_category: document.getElementById("filter-main-category").value,
    category: document.getElementById("filter-category").value,
    min_stars: starsVal > 0 ? starsVal : "",
    max_return: returnVal < 1.4 ? returnVal : "",
    min_reviews: reviewsVal > 0 ? reviewsVal : "",
    min_recommend: recommendVal > 0 ? recommendVal : "",
    sort: sortField,
    order: sortDir,
    source: document.getElementById("filter-source").value,
    keyword: activeKeyword,
    avoid: avoidMode ? "1" : "",
    page: currentPage
  };
}

// ---- API calls ----
async function fetchProducts() {
  const grid = document.getElementById("product-grid");
  grid.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>Loading products…</p></div>';

  const params = new URLSearchParams(getFilters());
  const res = await fetch(`/api/products?${params}`);
  const data = await res.json();
  renderProducts(data);
}

async function fetchCategories() {
  const src = document.getElementById("filter-source").value;
  const SOURCE_COUNTRY_MAP = { otto:"DE", warentest:"DE", amazon:"DE", ceneo:"PL",
                                dtest:"CZ", alza:"CZ", heureka:"CZ", zbozi:"CZ", datart:"CZ",
                                amazon_us:"US", heureka_sk:"SK", conrad:"DE" };
  // No source selected → no country filter → all-market categories
  const country = src ? (SOURCE_COUNTRY_MAP[src] || "") : "";
  const res = await fetch(`/api/categories${country ? "?country=" + country : ""}`);
  categoriesTree = await res.json();

  const mainSel = document.getElementById("filter-main-category");
  mainSel.innerHTML = '<option value="">All categories</option>' +
    categoriesTree.map(({ main, subs }) => {
      const total = subs.reduce((s, x) => s + x.count, 0);
      return `<option value="${escHtml(main)}">${escHtml(main)} (${total.toLocaleString()})</option>`;
    }).join("");
}

function populateSubcategories(mainValue) {
  const subGroup = document.getElementById("sub-category-group");
  const subSel   = document.getElementById("filter-category");

  if (!mainValue) {
    subGroup.style.display = "none";
    subSel.innerHTML = '<option value="">All subcategories</option>';
    return;
  }

  const entry = categoriesTree.find(e => e.main === mainValue);
  if (!entry) { subGroup.style.display = "none"; return; }

  subSel.innerHTML = '<option value="">All subcategories</option>' +
    entry.subs.map(({ sub, count }) =>
      `<option value="${escHtml(sub)}">${escHtml(sub)} (${count.toLocaleString()})</option>`
    ).join("");
  subGroup.style.display = "";
}

async function fetchKeywords() {
  const res = await fetch("/api/keywords");
  const data = await res.json();
  const container = document.getElementById("kw-filter-pills");
  if (!container) return;
  // Show top 20 keywords as clickable pills
  container.innerHTML = data.slice(0, 20).map(({ tag, count }) =>
    `<button class="kw-pill" data-kw="${escHtml(tag)}" title="${count} products">
       ${escHtml(tag)} <span class="kw-pill-count">${count}</span>
     </button>`
  ).join("");
  container.querySelectorAll(".kw-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      const kw = btn.dataset.kw;
      const clearBtn = document.getElementById("kw-clear-btn");
      if (activeKeyword === kw) {
        // deselect
        activeKeyword = "";
        btn.classList.remove("active");
        if (clearBtn) clearBtn.style.display = "none";
      } else {
        activeKeyword = kw;
        container.querySelectorAll(".kw-pill").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        if (clearBtn) clearBtn.style.display = "";
      }
      currentPage = 1;
      fetchProducts();
      if (window.innerWidth <= 900) closeSidebar();
    });
  });
}

async function fetchStats() {
  const res = await fetch("/api/stats");
  const d = await res.json();
  document.getElementById("stat-total").textContent = d.total.toLocaleString();
  document.getElementById("stat-stars").textContent = d.avg_stars ?? "—";
}

async function fetchCompetitorBrands() {
  try {
    const res = await fetch("/api/competitor-brands");
    const brands = await res.json();
    competitorBrands = new Set(brands);  // already lowercased on server
  } catch(e) { /* silent — badge just won't show */ }
}

// ---- Render ----
function starsVisual(rating) {
  if (!rating) return "—";
  const full = Math.floor(rating);
  const half = rating - full >= 0.3 ? 1 : 0;
  const empty = 5 - full - half;
  return "★".repeat(full) + (half ? "½" : "") + "☆".repeat(empty);
}

function returnClass(val) {
  if (val === null || val === undefined) return "";
  if (val === 0) return "good";
  if (val <= 0.5) return "good";
  if (val <= 1.0) return "warn";
  return "bad";
}

function recommendClass(val, isStars) {
  if (!val) return "";
  if (isStars) {
    if (val >= 4.5) return "good";
    if (val >= 4.0) return "warn";
    return "bad";
  }
  if (val >= 95) return "good";
  if (val >= 85) return "warn";
  return "bad";
}

function scoreColor(score) {
  if (score === null || score === undefined) return "var(--text3)";
  if (score >= 70) return "var(--green)";
  if (score >= 45) return "var(--yellow)";
  return "var(--red)";
}

function renderCard(p) {
  const returnRateDisplay = p.ReturnRate_pct !== null && p.ReturnRate_pct !== undefined
    ? p.ReturnRate_pct.toFixed(2) + "%" : "—";
  const starsDisplay = p.AvgStarRating ? p.AvgStarRating.toFixed(1) : "—";
  const useStarsForRec = !p.RecommendRate_pct && p.AvgStarRating;
  const recommendDisplay = p.RecommendRate_pct
    ? p.RecommendRate_pct + "%"
    : (p.AvgStarRating ? p.AvgStarRating.toFixed(1) + " ★" : "—");
  const recommendLabel = useStarsForRec ? "Rating" : "Recommend";
  const reviewsDisplay = p.ReviewsCount ? p.ReviewsCount.toLocaleString() : "—";
  const priceDisplay = priceStr(p);
  const sourceLabel = SOURCE_LABELS[p.source] || p.source || "Unknown";
  const sourceCls = p.source === "alza" ? "source-badge" : `source-badge scraper src-${p.source}`;
  const sourceBadge = `<span class="${sourceCls}">${sourceLabel}</span>`;

  const rankDisplay = (p.source_rank && p.source_total)
    ? `${p.source_rank.toLocaleString()} / ${p.source_total.toLocaleString()}`
    : "—";
  const rankClass = (p.source_rank && p.source_total)
    ? (p.source_rank <= Math.ceil(p.source_total * 0.1) ? "good"
      : p.source_rank >= Math.floor(p.source_total * 0.9) ? "bad" : "")
    : "";
  // Truncate long category names so they fit in the metric label
  const rankLabel = p.Category
    ? "in " + (p.Category.length > 18 ? p.Category.slice(0, 16) + "…" : p.Category)
    : "Rank";

  const firstMetric = p.source === "alza"
    ? `<div class="metric">
        <div class="metric-label">Return rate</div>
        <div class="metric-value ${returnClass(p.ReturnRate_pct)}">${returnRateDisplay}</div>
       </div>`
    : `<div class="metric">
        <div class="metric-label">${rankLabel}</div>
        <div class="metric-value ${rankClass}">${rankDisplay}</div>
       </div>`;

  const keywords = p.keywords ? JSON.parse(p.keywords) : [];
  const cardTags = keywords.slice(0, 2).map(k =>
    `<span class="kw-tag">${escHtml(k)}</span>`
  ).join("");

  const badge = qualityBadge(p);
  const brand = (p.Name || "").trim().split(/\s+/)[0].toLowerCase();
  const extBadge = competitorBrands.has(brand)
    ? `<span class="ext-badge" title="External ratings available">🌐 ext. ratings</span>`
    : "";

  return `
  <div class="product-card" onclick="openModal(${JSON.stringify(JSON.stringify(p))})">
    <div class="card-top-row">${sourceBadge}${badge}${extBadge}</div>
    <div class="card-category">${escHtml(p.Category || "")}</div>
    <div class="card-name">${escHtml(p.Name || "Unnamed")}</div>
    <div class="card-metrics">
      ${firstMetric}
      <div class="metric">
        <div class="metric-label">${recommendLabel}</div>
        <div class="metric-value ${recommendClass(useStarsForRec ? p.AvgStarRating : p.RecommendRate_pct, useStarsForRec)}">${recommendDisplay}</div>
      </div>
    </div>
    ${cardTags ? `<div class="card-tags">${cardTags}</div>` : ""}
    <div class="card-stars">
      <span class="stars-visual">${starsVisual(p.AvgStarRating)}</span>
      <span>${starsDisplay}</span>
      <span style="color:var(--text3)">(${reviewsDisplay} reviews)</span>
    </div>
    <div class="card-footer">
      <span class="card-price">${priceDisplay}</span>
      ${p.ProductURL ? `<a class="card-link" href="${escHtml(p.ProductURL)}" target="_blank" onclick="event.stopPropagation()">View →</a>` : ""}
    </div>
  </div>`;
}

function renderProducts(data) {
  const grid = document.getElementById("product-grid");
  const info = document.getElementById("results-info");

  if (isListView) grid.classList.add("list-view");
  else grid.classList.remove("list-view");

  if (!data.products.length) {
    grid.innerHTML = `
      <div class="empty-state">
        <div class="empty-state-icon">🔍</div>
        <h3>No products found</h3>
        <p>Try adjusting your filters or search term</p>
      </div>`;
    info.innerHTML = "No results";
    document.getElementById("pagination").innerHTML = "";
    return;
  }

  const start = (data.page - 1) * data.page_size + 1;
  const end = Math.min(data.page * data.page_size, data.total);
  info.innerHTML = `Showing <strong>${start}–${end}</strong> of <strong>${data.total.toLocaleString()}</strong> products`;

  grid.innerHTML = data.products.map(renderCard).join("");
  renderPagination(data.page, data.pages);
}

// ---- Pagination ----
function renderPagination(current, total) {
  const pag = document.getElementById("pagination");
  if (total <= 1) { pag.innerHTML = ""; return; }

  const pages = [];
  pages.push({ type: "btn", label: "‹", page: current - 1, disabled: current === 1 });

  const range = paginationRange(current, total);
  let prev = null;
  for (const p of range) {
    if (prev !== null && p - prev > 1) pages.push({ type: "ellipsis" });
    pages.push({ type: "btn", label: p, page: p, active: p === current });
    prev = p;
  }
  pages.push({ type: "btn", label: "›", page: current + 1, disabled: current === total });

  pag.innerHTML = pages.map(p => {
    if (p.type === "ellipsis") return `<span class="page-ellipsis">…</span>`;
    const cls = ["page-btn", p.active ? "active" : "", p.disabled ? "" : ""].filter(Boolean).join(" ");
    const disabled = p.disabled ? "disabled" : "";
    return `<button class="${cls}" ${disabled} onclick="goPage(${p.page})">${p.label}</button>`;
  }).join("");
}

function paginationRange(current, total) {
  const delta = 2;
  const left = current - delta, right = current + delta;
  const pages = new Set([1, total]);
  for (let i = left; i <= right; i++) if (i > 1 && i < total) pages.add(i);
  return Array.from(pages).sort((a, b) => a - b);
}

function goPage(page) {
  currentPage = page;
  fetchProducts();
  window.scrollTo({ top: 200, behavior: "smooth" });
}

// ---- Modal ----
function openModal(jsonStr) {
  const p = JSON.parse(jsonStr);
  const overlay = document.getElementById("modal-overlay");
  const content = document.getElementById("modal-content");

  // Parse details_json for source-specific extras
  let dj = {};
  try { dj = p.details_json ? JSON.parse(p.details_json) : {}; } catch(e) {}

  // Star bars — ceneo stores star_distribution as percentages; others use raw counts
  let starBars = "";
  if (p.source === "ceneo") {
    const sd = dj.star_distribution || {};
    starBars = [5,4,3,2,1].map(n => {
      const pct = sd[String(n)] || 0;
      return `
      <div class="star-bar-row">
        <span class="star-bar-label">★${n}</span>
        <div class="star-bar-track"><div class="star-bar-fill" style="width:${pct}%"></div></div>
        <span class="star-bar-count">${pct}%</span>
      </div>`;
    }).join("");
  } else {
    const totalRatings = (p.Stars5_Count || 0) + (p.Stars4_Count || 0) + (p.Stars3_Count || 0)
      + (p.Stars2_Count || 0) + (p.Stars1_Count || 0);
    if (totalRatings > 0) {
      starBars = [5,4,3,2,1].map(n => {
        const cnt = p[`Stars${n}_Count`] || 0;
        const pct = Math.round(cnt / totalRatings * 100);
        return `
        <div class="star-bar-row">
          <span class="star-bar-label">★${n}</span>
          <div class="star-bar-track"><div class="star-bar-fill" style="width:${pct}%"></div></div>
          <span class="star-bar-count">${cnt}</span>
        </div>`;
      }).join("");
    }
  }

  // Ceneo feature scores block
  const featScores = dj.feature_scores || {};
  const featKeys = Object.keys(featScores);
  const featBlock = featKeys.length > 0 ? `
    <div class="modal-keywords" style="margin-top:12px">
      <div class="modal-keywords-label">User ratings by feature</div>
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:6px;margin-top:6px">
        ${featKeys.map(k => `
          <div style="background:var(--surface2);border-radius:6px;padding:6px 8px;font-size:12px">
            <div style="color:var(--text2);margin-bottom:2px">${escHtml(k)}</div>
            <div style="font-weight:600;color:${featScores[k]>=90?'var(--green)':featScores[k]>=75?'var(--amber)':'var(--red)'}">${featScores[k]}%</div>
          </div>`).join("")}
      </div>
    </div>` : "";

  const totalRatings = 0; // used below only for the old path (now handled above)

  const keywords = p.keywords ? JSON.parse(p.keywords) : [];

  content.innerHTML = `
    <div class="modal-category">${escHtml(p.Category || "")}</div>
    <div class="modal-name">${escHtml(p.Name || "Unnamed")}</div>

    <div class="modal-stars-row">
      <span class="modal-stars-big">${p.AvgStarRating ? p.AvgStarRating.toFixed(1) : "—"}</span>
      <span class="modal-stars-visual">${starsVisual(p.AvgStarRating)}</span>
      <span class="modal-reviews-count">${p.ReviewsCount ? p.ReviewsCount.toLocaleString() + " reviews" : ""}</span>
    </div>

    <div class="modal-metrics">
      <div class="modal-metric">
        <div class="modal-metric-label">Return Rate</div>
        <div class="modal-metric-value ${returnClass(p.ReturnRate_pct)}">
          ${p.ReturnRate_pct !== null && p.ReturnRate_pct !== undefined ? p.ReturnRate_pct.toFixed(2) + "%" : "—"}
        </div>
      </div>
      <div class="modal-metric">
        <div class="modal-metric-label">Recommend</div>
        <div class="modal-metric-value ${recommendClass(p.RecommendRate_pct)}">
          ${p.RecommendRate_pct ? p.RecommendRate_pct + "%" : "—"}
        </div>
      </div>
      <div class="modal-metric">
        <div class="modal-metric-label">Price</div>
        <div class="modal-metric-value">${priceStr(p) || "—"}</div>
      </div>
    </div>

    ${starBars ? `<div class="star-bar-wrap">${starBars}</div>` : ""}

    ${featBlock}

    ${p.Description ? `<div class="modal-desc">${escHtml(p.Description).substring(0, 500)}${p.Description.length > 500 ? "…" : ""}</div>` : ""}

    ${keywords.length > 0 ? `
    <div class="modal-keywords">
      <div class="modal-keywords-label">Quality signals</div>
      <div class="modal-keywords-tags">
        ${keywords.map(k => `<span class="kw-tag kw-tag-modal">${escHtml(k)}</span>`).join("")}
      </div>
    </div>` : ""}

    <div id="competitor-scores-section" class="competitor-section">
      <div class="competitor-section-title">🌐 External ratings</div>
      <div id="competitor-scores-body" class="competitor-loading">Loading…</div>
    </div>

    <div class="modal-actions">
      ${p.ProductURL ? `<a class="btn-primary" href="${escHtml(p.ProductURL)}" target="_blank">View on ${SOURCE_LABELS[p.source] || "Shop"} →</a>` : ""}
      <button class="btn-secondary" onclick="closeModal()">Close</button>
    </div>`;

  overlay.classList.add("open");

  // Async: fetch competitor scores for this product's brand
  const brand = (p.Name || "").trim().split(/\s+/)[0];
  if (brand && brand.length >= 2) {
    fetch(`/api/competitor-scores?brand=${encodeURIComponent(brand)}`)
      .then(r => r.json())
      .then(scores => renderCompetitorScores(scores, p.Category))
      .catch(() => {
        const el = document.getElementById("competitor-scores-body");
        if (el) el.innerHTML = '<span class="competitor-none">No external data</span>';
      });
  } else {
    const el = document.getElementById("competitor-scores-body");
    if (el) el.innerHTML = '<span class="competitor-none">No external data</span>';
  }
}

function renderCompetitorScores(scores, productCategory) {
  const el = document.getElementById("competitor-scores-body");
  if (!el) return;

  if (!scores || scores.length === 0) {
    el.innerHTML = '<span class="competitor-none">No external ratings found for this brand</span>';
    return;
  }

  // Group by source
  const bySource = {};
  for (const s of scores) {
    if (!bySource[s.source]) bySource[s.source] = [];
    bySource[s.source].push(s);
  }

  const html = Object.entries(bySource).map(([source, items]) => {
    const meta = COMPETITOR_SOURCE_META[source] || { label: source, color: "#6b7280", desc: "" };
    const rows = items.map(item => {
      const bar = Math.round(item.score);
      return `
        <div class="comp-row">
          <div class="comp-category">${escHtml(item.category)}</div>
          <div class="comp-bar-wrap">
            <div class="comp-bar" style="width:${bar}%;background:${scoreColor(item.score)}"></div>
          </div>
          <div class="comp-score" style="color:${scoreColor(item.score)}">${item.score}</div>
          <div class="comp-n">(n=${item.n})</div>
        </div>`;
    }).join("");
    return `
      <div class="comp-source-block">
        <div class="comp-source-name" style="color:${meta.color}" title="${meta.desc}">${meta.label}</div>
        ${rows}
      </div>`;
  }).join("");

  el.innerHTML = html;
}

function closeModal() {
  document.getElementById("modal-overlay").classList.remove("open");
}

// ---- Helpers ----
function escHtml(str) {
  return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function triggerSearch() {
  currentPage = 1;
  fetchProducts();
}

function debouncedSearch() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(triggerSearch, 350);
}

// ---- BRANDS TAB ----

function switchTab(tab) {
  const isProducts = tab === 'products';
  document.getElementById('tab-products').classList.toggle('active', isProducts);
  document.getElementById('tab-brands').classList.toggle('active', !isProducts);

  // Show/hide the main layout and brands panel
  document.querySelector('.layout').style.display     = isProducts ? '' : 'none';
  document.querySelector('.mobile-filter-btn').style.display = isProducts ? '' : 'none';
  document.getElementById('brands-panel').style.display     = isProducts ? 'none' : '';
  document.getElementById('pagination').style.display       = isProducts ? '' : 'none';

  if (!isProducts) loadBrandsTab();
}

let brandsLoaded = false;

async function loadBrandsTab() {
  // Populate category dropdown once
  if (!brandsLoaded) {
    try {
      const res = await fetch('/api/brand-categories');
      const cats = await res.json();
      const sel = document.getElementById('brands-category');
      sel.innerHTML = '<option value="">All categories</option>' +
        cats.map(c => `<option value="${escHtml(c)}">${escHtml(c)}</option>`).join('');
    } catch(e) {}

    document.getElementById('brands-search').addEventListener('input', debounceLoadBrands);
    document.getElementById('brands-category').addEventListener('change', fetchBrands);
    document.getElementById('brands-sort').addEventListener('change', fetchBrands);
    brandsLoaded = true;
  }
  fetchBrands();
}

let brandsDebounce = null;
function debounceLoadBrands() {
  clearTimeout(brandsDebounce);
  brandsDebounce = setTimeout(fetchBrands, 300);
}

async function fetchBrands() {
  document.getElementById('brands-tbody').innerHTML =
    '<tr><td colspan="8" style="text-align:center;padding:40px;color:var(--text3)">Loading…</td></tr>';

  const q    = document.getElementById('brands-search').value.trim();
  const cat  = document.getElementById('brands-category').value;
  const sort = document.getElementById('brands-sort').value;
  const params = new URLSearchParams({ q, category: cat, sort });

  try {
    const res = await fetch(`/api/brands?${params}`);
    const brands = await res.json();
    renderBrands(brands);
  } catch(e) {
    document.getElementById('brands-tbody').innerHTML =
      '<tr><td colspan="8" style="text-align:center;padding:40px;color:var(--text3)">Error loading data</td></tr>';
  }
}

function scoreBadge(val) {
  if (val === null || val === undefined) return '<span class="brand-score brand-score-none">—</span>';
  const cls = val >= 70 ? 'good' : val >= 45 ? 'warn' : 'bad';
  const bar = Math.round(val);
  return `<span class="brand-score brand-score-${cls}">
    <span class="brand-bar" style="width:${bar}%"></span>
    <span class="brand-score-val">${val}</span>
  </span>`;
}

function renderBrands(brands) {
  const tbody = document.getElementById('brands-tbody');
  document.getElementById('brands-count').textContent = `${brands.length} brands`;

  if (!brands.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:40px;color:var(--text3)">No brands found</td></tr>';
    return;
  }

  const rows = brands.map(b => {
    const cats = b.categories
      ? b.categories.split(',').filter(Boolean).slice(0, 3).map(c =>
          `<span class="brand-cat-tag">${escHtml(c.trim())}</span>`).join('')
      : '';
    const sourceDots = b.num_sources >= 3 ? '●●●' : b.num_sources === 2 ? '●●○' : '●○○';
    return `<tr>
      <td>
        <div class="brand-name-cell">
          <span class="brand-name">${escHtml(b.brand)}</span>
          <span class="brand-sources" title="${b.num_sources} sources, ${b.total_records} records">${sourceDots}</span>
        </div>
      </td>
      <td><div class="brand-cats">${cats}</div></td>
      <td>${scoreBadge(b.french_score)}</td>
      <td>${scoreBadge(b.openrepair_score)}</td>
      <td>${scoreBadge(b.yale_score)}</td>
      <td>${scoreBadge(b.ifixit_score)}</td>
      <td>${scoreBadge(b.bifl_score)}</td>
      <td>${scoreBadge(b.avg_score)}</td>
    </tr>`;
  }).join('');

  tbody.innerHTML = rows;
}

// ---- Event listeners ----
document.addEventListener("DOMContentLoaded", () => {
  buildSortOptions();   // populate sort dropdown before first fetch
  fetchStats();
  fetchCategories();
  fetchKeywords();
  fetchCompetitorBrands();
  fetchProducts();

  // Search
  document.getElementById("search-input").addEventListener("input", function() {
    document.getElementById("search-clear").style.display = this.value ? "block" : "none";
    debouncedSearch();
  });
  document.getElementById("search-clear").addEventListener("click", () => {
    document.getElementById("search-input").value = "";
    document.getElementById("search-clear").style.display = "none";
    triggerSearch();
  });

  // Sort & dropdowns (mobile versions registered later with closeSidebar)

  // Ranges
  const starSlider = document.getElementById("filter-stars");
  starSlider.addEventListener("input", () => {
    const v = parseFloat(starSlider.value);
    document.getElementById("stars-val").textContent = v > 0 ? v.toFixed(1) + "★" : "Any";
    debouncedSearch();
  });

  const returnSlider = document.getElementById("filter-return");
  returnSlider.addEventListener("input", () => {
    const v = parseFloat(returnSlider.value);
    document.getElementById("return-val").textContent = v.toFixed(1) + "%";
    debouncedSearch();
  });

  const reviewsSlider = document.getElementById("filter-reviews");
  reviewsSlider.addEventListener("input", () => {
    const v = parseInt(reviewsSlider.value);
    document.getElementById("reviews-val").textContent = v > 0 ? v + "+" : "Any";
    debouncedSearch();
  });

  const recommendSlider = document.getElementById("filter-recommend");
  recommendSlider.addEventListener("input", () => {
    const v = parseInt(recommendSlider.value);
    document.getElementById("recommend-val").textContent = v > 0 ? v + "%+" : "Any";
    debouncedSearch();
  });

  // Quick star buttons
  document.querySelectorAll(".star-btn[data-val]").forEach(btn => {
    btn.addEventListener("click", () => {
      const v = parseFloat(btn.dataset.val);
      starSlider.value = v;
      document.getElementById("stars-val").textContent = v.toFixed(1) + "★";
      document.querySelectorAll(".star-btn[data-val]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      triggerSearch();
    });
  });

  // Quick return buttons
  document.querySelectorAll(".star-btn[data-return]").forEach(btn => {
    btn.addEventListener("click", () => {
      const v = parseFloat(btn.dataset.return);
      returnSlider.value = v;
      document.getElementById("return-val").textContent = v.toFixed(1) + "%";
      document.querySelectorAll(".star-btn[data-return]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      triggerSearch();
    });
  });

  // Products to avoid toggle
  const avoidToggle = document.getElementById("avoid-toggle");
  const avoidInfo   = document.getElementById("avoid-info");
  if (avoidToggle) {
    avoidToggle.addEventListener("click", () => {
      avoidMode = !avoidMode;
      avoidToggle.dataset.active = avoidMode ? "1" : "0";
      avoidToggle.textContent = avoidMode ? "⚠️ Showing products to avoid" : "Show products to avoid";
      avoidToggle.classList.toggle("avoid-btn-active", avoidMode);
      if (avoidInfo) avoidInfo.style.display = avoidMode ? "" : "none";
      currentPage = 1;
      triggerSearch();
    });
  }

  // Clear keyword filter button
  const kwClearBtn = document.getElementById("kw-clear-btn");
  if (kwClearBtn) {
    kwClearBtn.addEventListener("click", () => {
      activeKeyword = "";
      document.querySelectorAll(".kw-pill").forEach(b => b.classList.remove("active"));
      kwClearBtn.style.display = "none";
      currentPage = 1;
      triggerSearch();
    });
  }

  // Reset
  document.getElementById("reset-filters").addEventListener("click", () => {
    document.getElementById("search-input").value = "";
    document.getElementById("search-clear").style.display = "none";
    document.getElementById("filter-main-category").value = "";
    populateSubcategories("");   // hides sub-dropdown and clears it
    document.getElementById("filter-source").value = "";
    activeKeyword = "";
    document.querySelectorAll(".kw-pill").forEach(b => b.classList.remove("active"));
    if (kwClearBtn) kwClearBtn.style.display = "none";
    // Reset avoid mode
    avoidMode = false;
    if (avoidToggle) { avoidToggle.dataset.active = "0"; avoidToggle.textContent = "Show products to avoid"; avoidToggle.classList.remove("avoid-btn-active"); }
    if (avoidInfo) avoidInfo.style.display = "none";
    document.getElementById("sort-by").value = "RecommendRate_pct_desc";
    // Restore recommend filter (hidden when amazon_us was selected)
    const recGroup = document.getElementById("recommend-filter-group");
    if (recGroup) recGroup.style.display = "";
    starSlider.value = 0; document.getElementById("stars-val").textContent = "Any";
    returnSlider.value = 1.4; document.getElementById("return-val").textContent = "1.4%";
    reviewsSlider.value = 0; document.getElementById("reviews-val").textContent = "Any";
    recommendSlider.value = 0; document.getElementById("recommend-val").textContent = "Any";
    document.querySelectorAll(".star-btn").forEach(b => b.classList.remove("active"));
    currentPage = 1;
    fetchProducts();
  });

  // View toggle
  document.getElementById("view-grid").addEventListener("click", () => {
    isListView = false;
    document.getElementById("view-grid").classList.add("active");
    document.getElementById("view-list").classList.remove("active");
    document.getElementById("product-grid").classList.remove("list-view");
  });
  document.getElementById("view-list").addEventListener("click", () => {
    isListView = true;
    document.getElementById("view-list").classList.add("active");
    document.getElementById("view-grid").classList.remove("active");
    document.getElementById("product-grid").classList.add("list-view");
    triggerSearch();
  });

  // Modal close
  document.getElementById("modal-close").addEventListener("click", closeModal);
  document.getElementById("modal-overlay").addEventListener("click", e => {
    if (e.target === e.currentTarget) closeModal();
  });
  document.addEventListener("keydown", e => {
    if (e.key === "Escape") { closeModal(); closeSidebar(); }
  });

  // Mobile sidebar toggle
  function openSidebar() {
    document.getElementById("sidebar").classList.add("open");
    document.getElementById("sidebar-backdrop").classList.add("open");
    document.body.style.overflow = "hidden";
  }
  function closeSidebar() {
    document.getElementById("sidebar").classList.remove("open");
    document.getElementById("sidebar-backdrop").classList.remove("open");
    document.body.style.overflow = "";
  }
  document.getElementById("mobile-filter-btn").addEventListener("click", openSidebar);
  document.getElementById("sidebar-backdrop").addEventListener("click", closeSidebar);
  document.getElementById("sidebar-close").addEventListener("click", closeSidebar);

  // Auto-close sidebar after applying a filter on mobile
  function triggerSearchAndClose() {
    triggerSearch();
    if (window.innerWidth <= 900) closeSidebar();
  }

  // Main category → populate subcategory dropdown, then search
  document.getElementById("filter-main-category").addEventListener("change", function() {
    populateSubcategories(this.value);
    currentPage = 1;
    triggerSearchAndClose();
  });

  document.getElementById("filter-source").addEventListener("change", () => {
    const src = document.getElementById("filter-source").value;
    const isAmazonUS = src === "amazon_us";

    // Auto-switch sort to "Most reviewed" for amazon_us (RecommendRate is always null there)
    const sortSel = document.getElementById("sort-by");
    if (isAmazonUS && sortSel.value === "RecommendRate_pct_desc") {
      sortSel.value = "ReviewsCount_desc";
    } else if (!isAmazonUS && sortSel.value === "ReviewsCount_desc") {
      sortSel.value = "RecommendRate_pct_desc";
    }

    // Hide recommend rate filter for amazon_us (all products have NULL there)
    const recGroup = document.getElementById("recommend-filter-group");
    if (recGroup) recGroup.style.display = isAmazonUS ? "none" : "";

    // Re-fetch categories in the correct country when source changes
    document.getElementById("filter-main-category").value = "";
    populateSubcategories("");
    fetchCategories();
    triggerSearchAndClose();
  });
  ["filter-category", "sort-by"].forEach(id => {
    document.getElementById(id).addEventListener("change", triggerSearchAndClose);
  });
  [starSlider, returnSlider, reviewsSlider, recommendSlider].forEach(sl => {
    sl.addEventListener("change", () => { if (window.innerWidth <= 900) closeSidebar(); });
  });
});
