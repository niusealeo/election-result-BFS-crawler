/**
 * sink.js
 *
 * Directory rule (minimal):
 * downloads/
 *   term_54_(2023)/
 *     <term-level files: referendums, nationwide tables, etc.>
 *     068_Tāmaki_Makaurau/
 *       <electorate-scoped files and by-election files within this term>
 *
 * No "general-election", "electorates", "referendums", "by-elections" folders.
 * Overwrites on collision.
 *
 * Endpoints:
 *  - POST /meta/electorates : store dual-map {termKey, official_order, alphabetical_order}
 *  - POST /sink            : receive one file via multipart and save to correct term/electorate folder
 *  - GET  /meta/electorates: read current mappings
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const multer = require("multer");
const sanitize = require("sanitize-filename");

const app = express();
app.use(express.json({ limit: "20mb" })); // meta payload only; file uploads are multipart

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 250 * 1024 * 1024 }, // 250MB per file
});

const DOWNLOADS_ROOT = path.resolve(process.env.DOWNLOADS_ROOT || "./downloads");
const META_DIR = path.join(DOWNLOADS_ROOT, "_meta");
const META_PATH = path.join(META_DIR, "electorates_by_term.json");
const UNRESOLVED_JSONL = path.join(META_DIR, "unresolved_urls.jsonl");

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
ensureDir(DOWNLOADS_ROOT);
ensureDir(META_DIR);

function readJsonSafe(p, fallback) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}
function writeJson(p, obj) {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}
function appendJsonl(p, obj) {
  ensureDir(path.dirname(p));
  fs.appendFileSync(p, JSON.stringify(obj) + "\n", "utf8");
}

function safePart(s) {
  // Keep macrons; remove path separators and illegal OS chars
  return sanitize(String(s || "")).replace(/\s+/g, "_");
}

function normalizeUrl(u) {
  try {
    const U = new URL(u);
    // normalize dot segments in path
    const parts = U.pathname.split("/").filter(Boolean);
    const stack = [];
    for (const part of parts) {
      if (part === ".") continue;
      if (part === "..") stack.pop();
      else stack.push(part);
    }
    U.pathname = "/" + stack.join("/");
    return U.toString();
  } catch {
    return String(u || "");
  }
}

const TERM_BY_GE_YEAR = {
  1996: 45,
  1999: 46,
  2002: 47,
  2005: 48,
  2008: 49,
  2011: 50,
  2014: 51,
  2017: 52,
  2020: 53,
  2023: 54,
};

// A by-election year belongs to a parent term’s GE year.
// Extend as needed.
const BYELECTION_YEAR_TO_PARENT_GE_YEAR = {
  2025: 2023,
};

function geYearFromUrl(u) {
  const m = u.match(/electionresults_(\d{4})/i);
  return m ? Number(m[1]) : null;
}
function byElectionYearFromUrl(u) {
  // You’ve got patterns like /2025_tamaki_makaurau_byelection/...
  let m = u.match(/\/(20\d{2})_[^/]+_byelection/i);
  if (m) return Number(m[1]);
  // fallback any 4-digit year
  m = u.match(/(19\d{2}|20\d{2})/);
  return m ? Number(m[1]) : null;
}

function termKeyForUrl(u) {
  const ge = geYearFromUrl(u);
  if (ge && TERM_BY_GE_YEAR[ge]) return `term_${TERM_BY_GE_YEAR[ge]}_(${ge})`;

  const by = byElectionYearFromUrl(u);
  const parentGe = by ? BYELECTION_YEAR_TO_PARENT_GE_YEAR[by] : null;
  if (parentGe && TERM_BY_GE_YEAR[parentGe]) return `term_${TERM_BY_GE_YEAR[parentGe]}_(${parentGe})`;

  // referendums: often /2013_*_referendum...  -> term by year range is ambiguous without a range table.
  // For your core set: 2013 should be term_50_(2011). We can route by year bands:
  const y = by || ge || null;
  if (y != null) {
    // Minimal ranges (extend if you add more years)
    if (y >= 2023 && y <= 2026) return "term_54_(2023)";
    if (y >= 2020 && y <= 2023) return "term_53_(2020)";
    if (y >= 2017 && y <= 2020) return "term_52_(2017)";
    if (y >= 2014 && y <= 2017) return "term_51_(2014)";
    if (y >= 2011 && y <= 2014) return "term_50_(2011)"; // covers 2013 referendum as requested
    if (y >= 2008 && y <= 2011) return "term_49_(2008)";
    if (y >= 2005 && y <= 2008) return "term_48_(2005)";
    if (y >= 2002 && y <= 2005) return "term_47_(2002)";
    if (y >= 1999 && y <= 2002) return "term_46_(1999)";
    if (y >= 1996 && y <= 1999) return "term_45_(1996)";
  }

  return null;
}

// Detect electorate number from URL when present (many GE pages embed it)
function electorateNumFromUrl(u) {
  let m = u.match(/electorate-details-(\d+)\.html/i);
  if (m) return Number(m[1]);

  m = u.match(/split-votes-electorate-(\d+)\.html/i);
  if (m) return Number(m[1]);

  // add patterns if your csv filenames include electorate numbers
  return null;
}

function loadMeta() {
  return readJsonSafe(META_PATH, {}); // { termKey: { official_order: { "1":"Name",... }, alphabetical_order:{...} } }
}
function saveMeta(meta) {
  writeJson(META_PATH, meta);
}

// Use number->name map; if URL doesn't include number, try matching by slugged name (best-effort)
function inferElectorateFolder(termKey, url, meta) {
  const term = meta[termKey];
  if (!term || !term.official_order) return null;

  const official = term.official_order; // { "68":"Tāmaki Makaurau", ... } but could be string keys

  // 1) if URL includes electorate number
  const num = electorateNumFromUrl(url);
  if (num != null) {
    const name = official[String(num)];
    if (name) return `${String(num).padStart(3, "0")}_${safePart(name)}`;
    // still make folder with unknown name
    return `${String(num).padStart(3, "0")}_Unknown`;
  }

  // 2) fallback: match by name slug in URL
  const urlLower = decodeURIComponent(url).toLowerCase().replace(/_/g, "-");
  for (const [k, name] of Object.entries(official)) {
    const slug = String(name)
      .toLowerCase()
      .replace(/[\u2019']/g, "")
      .replace(/\s+/g, "-");
    if (urlLower.includes(slug)) {
      const n = Number(k);
      const nStr = Number.isFinite(n) ? String(n).padStart(3, "0") : "UNK";
      return `${nStr}_${safePart(name)}`;
    }
  }

  return null;
}

// -------------------- META INGEST --------------------
app.post("/meta/electorates", (req, res) => {
  const { termKey, official_order, alphabetical_order } = req.body || {};
  if (!termKey || !official_order || !alphabetical_order) {
    return res.status(400).json({
      ok: false,
      error: "Expected { termKey, official_order, alphabetical_order }",
    });
  }

  const meta = loadMeta();
  meta[termKey] = { official_order, alphabetical_order };
  saveMeta(meta);

  res.json({ ok: true, termKey, official_count: Object.keys(official_order).length });
});

app.get("/meta/electorates", (_req, res) => {
  res.json(loadMeta());
});

// -------------------- FILE SINK --------------------
// multipart fields: url, filename (optional), file (binary)
app.post("/sink", upload.single("file"), (req, res) => {
  const urlRaw = req.body?.url;
  if (!urlRaw) return res.status(400).json({ ok: false, error: "Missing form field: url" });
  if (!req.file?.buffer) return res.status(400).json({ ok: false, error: "Missing multipart file field: file" });

  const url = normalizeUrl(urlRaw);
  const meta = loadMeta();

  const termKey = termKeyForUrl(url);
  if (!termKey) {
    // save to unresolved and log
    const outDir = path.join(DOWNLOADS_ROOT, "_unresolved");
    ensureDir(outDir);

    const fn = safePart(req.body?.filename || req.file.originalname || "download.bin");
    const outPath = path.join(outDir, fn);
    fs.writeFileSync(outPath, req.file.buffer);

    appendJsonl(UNRESOLVED_JSONL, { ts: new Date().toISOString(), url, saved_as: outPath, why: "no termKey" });

    return res.json({ ok: true, saved_as: outPath, termKey: null, electorate: null, note: "unresolved term" });
  }

  const termDir = path.join(DOWNLOADS_ROOT, termKey);
  ensureDir(termDir);

  const electorateFolder = inferElectorateFolder(termKey, url, meta);
  const outDir = electorateFolder ? path.join(termDir, electorateFolder) : termDir;
  ensureDir(outDir);

  const filename = safePart(req.body?.filename || req.file.originalname || "download.bin");
  const outPath = path.join(outDir, filename);

  // overwrite
  fs.writeFileSync(outPath, req.file.buffer);

  res.json({ ok: true, saved_as: outPath, termKey, electorate: electorateFolder || null });
});

app.get("/health", (_req, res) => res.json({ ok: true }));

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`sink listening on http://localhost:${PORT}`);
  console.log(`downloads root: ${DOWNLOADS_ROOT}`);
});
