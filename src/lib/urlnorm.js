const { URL } = require("url");

function normalizeUrl(u) {
  try {
    const U = new URL(String(u).trim());
    // remove hash; keep query
    U.hash = "";
    // normalize /index.html to /
    if (U.pathname.endsWith("/index.html")) U.pathname = U.pathname.replace(/\/index\.html$/, "/");
    // collapse // in path
    U.pathname = U.pathname.replace(/\/\/{2,}/g, "/");
    return U.toString();
  } catch {
    return String(u || "").trim();
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
