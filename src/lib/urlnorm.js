const { URL } = require("url");

// We sometimes ingest URLs scraped from HTML where query separators were
// HTML-escaped (&amp;) and then double-encoded (e.g. "amp%3B").
// That can explode the crawl graph with semantically-identical URLs.
// We keep query parameters (they can be meaningful) but clean *only* the
// obvious entity leakage.
function cleanAmpArtifacts(input) {
  let s = String(input ?? "").trim();
  if (!s) return s;

  // Iterate until stable (cap to avoid pathological loops)
  for (let i = 0; i < 8; i++) {
    const prev = s;

    // Common HTML entity inside href attributes
    s = s.replace(/&amp;/g, "&");

    // Percent-encoded "&amp;" turns into "%26amp%3B" in some crawls
    s = s.replace(/%26amp%3B/gi, "&");

    // The stray "amp;" token itself (and its percent-encoded "amp%3B")
    s = s.replace(/amp%3B/gi, "");
    s = s.replace(/amp;/g, "");

    if (s === prev) break;
  }

  return s;
}

function dedupeIdenticalQueryPairs(U) {
  // Remove only exact duplicate key/value pairs (safe).
  // Keep duplicates with differing values (might be meaningful or backend-dependent).
  try {
    const cleaned = new URL(U.toString());
    cleaned.search = "";

    const seen = new Map(); // key -> Set(values)
    for (const [k, v] of U.searchParams.entries()) {
      const key = String(k);
      const val = String(v);
      if (!seen.has(key)) seen.set(key, new Set());
      const set = seen.get(key);
      if (!set.has(val)) {
        set.add(val);
        cleaned.searchParams.append(key, val);
      }
    }
    return cleaned;
  } catch {
    return U;
  }
}

function normalizeUrl(u) {
  try {
    const cleanedInput = cleanAmpArtifacts(u);
    let U = new URL(String(cleanedInput).trim());
    // remove hash; keep query
    U.hash = "";
    // normalize /index.html to /
    if (U.pathname.endsWith("/index.html")) U.pathname = U.pathname.replace(/\/index\.html$/, "/");
    // collapse // in path
    U.pathname = U.pathname.replace(/\/\/{2,}/g, "/");

    // Safely dedupe identical repeated query pairs (e.g. start=5&start=5)
    U = dedupeIdenticalQueryPairs(U);
    return U.toString();
  } catch {
    return cleanAmpArtifacts(String(u || "").trim());
  }
}

function extFromUrl(u) {
  const m = String(u).match(/\.([a-z0-9]+)(?:\?|#|$)/i);
  return m ? m[1].toLowerCase() : "bin";
}

function stableUniqUrls(urls) {
  const seen = new Set();
  const out = [];
  for (const x of Array.isArray(urls) ? urls : []) {
    const u = normalizeUrl(x);
    if (!u) continue;
    if (!seen.has(u)) {
      seen.add(u);
      out.push(u);
    }
  }
  return out;
}

module.exports = { normalizeUrl, extFromUrl, stableUniqUrls };
