/**
 * sink.js — single server for:
 *  - electorate maps (dual structure per term)
 *  - saving downloaded files to term/electorate folders (overwrite)
 *  - persisting Postman runner datafiles (urls-level / files-level) + run logs
 *
 * Install:
 *   npm i express multer sanitize-filename
 * Run:
 *   node sink.js
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const multer = require("multer");
const sanitize = require("sanitize-filename");

const app = express();
app.use(express.json({ limit: "200mb" })); // artifacts can be big

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 300 * 1024 * 1024 }, // 300MB per file
});

const DOWNLOADS_ROOT = path.resolve(process.env.DOWNLOADS_ROOT || "./downloads");
const META_DIR = path.join(DOWNLOADS_ROOT, "_meta");
const ARTIFACT_DIR = path.join(META_DIR, "artifacts");
const ELECTORATES_META_PATH = path.join(META_DIR, "electorates_by_term.json");

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
ensureDir(DOWNLOADS_ROOT);
ensureDir(META_DIR);
ensureDir(ARTIFACT_DIR);

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
  // keep macrons; sanitize only filesystem illegal chars
  return sanitize(String(s || "")).replace(/\s+/g, "_");
}

function normalizeUrl(u) {
  try {
    const U = new URL(u);
    // normalize dot segments
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

// GE year -> term number
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

// Referendums / by-elections: map event year -> parent GE year (extend as needed)
const YEAR_TO_PARENT_GE_YEAR = {
  2013: 2011, // your rule: 2013 referendum in term_50_(2011)
  2025: 2023, // your rule: 2025 by-election in term_54_(2023)
};

function termKeyFromGeYear(geYear) {
  const term = TERM_BY_GE_YEAR[geYear];
  if (!term) return null;
  return `term_${term}_(${geYear})`;
}

function geYearFromUrl(u) {
  const m = String(u).match(/electionresults_(\d{4})/i);
  return m ? Number(m[1]) : null;
}

function anyYearFromUrl(u) {
  const m = String(u).match(/(19\d{2}|20\d{2})/);
  return m ? Number(m[1]) : null;
}

function termKeyForUrl(u) {
  const ge = geYearFromUrl(u);
  if (ge && TERM_BY_GE_YEAR[ge]) return termKeyFromGeYear(ge);

  const y = anyYearFromUrl(u);
  if (y && YEAR_TO_PARENT_GE_YEAR[y]) return termKeyFromGeYear(YEAR_TO_PARENT_GE_YEAR[y]);

  // fallback term bands (minimal, extends if you add older years)
  if (y != null) {
    if (y >= 2023 && y <= 2026) return "term_54_(2023)";
    if (y >= 2020 && y <= 2023) return "term_53_(2020)";
    if (y >= 2017 && y <= 2020) return "term_52_(2017)";
    if (y >= 2014 && y <= 2017) return "term_51_(2014)";
    if (y >= 2011 && y <= 2014) return "term_50_(2011)";
    if (y >= 2008 && y <= 2011) return "term_49_(2008)";
    if (y >= 2005 && y <= 2008) return "term_48_(2005)";
    if (y >= 2002 && y <= 2005) return "term_47_(2002)";
    if (y >= 1999 && y <= 2002) return "term_46_(1999)";
    if (y >= 1996 && y <= 1999) return "term_45_(1996)";
  }

  return null;
}

// Detect electorate number from URL when present
function electorateNumFromUrl(u) {
  // 2017+ commonly: electorate-details-68.html
  let m = String(u).match(/electorate-details-(\d+)\.html/i);
  if (m) return Number(m[1]);

  // Some pages: split-votes-electorate-28.html
  m = String(u).match(/split-votes-electorate-(\d+)\.html/i);
  if (m) return Number(m[1]);

  // Older: electorate-27.html / electorate-27-something.html
  m = String(u).match(/electorate-(\d+)[^\/]*\.html/i);
  if (m) return Number(m[1]);

  return null;
}

// Term meta storage
function loadElectoratesMeta() {
  return readJsonSafe(ELECTORATES_META_PATH, {}); // { termKey: { official_order, alphabetical_order } }
}
function saveElectoratesMeta(meta) {
  writeJson(ELECTORATES_META_PATH, meta);
}

// Clean name helper (removes junk suffix you currently have for 1996)
function cleanElectorateName(name) {
  return String(name || "")
    .replace(/\s*\(\.pdf[^)]*\)\s*$/i, "") // remove "(.pdf 338KB)"
    .replace(/\s+/g, " ")
    .trim();
}

// Given termKey + URL, return electorate folder name NNN_Name or null (term-level)
function inferElectorateFolder(termKey, url, meta) {
  const termMeta = meta[termKey];
  const official = termMeta?.official_order; // { "1":"Auckland Central", ... }
  if (!official) return null;

  const num = electorateNumFromUrl(url);
  if (num != null) {
    const name = official[String(num)];
    if (!name) return `${String(num).padStart(3, "0")}_Unknown`;
    return `${String(num).padStart(3, "0")}_${safePart(cleanElectorateName(name))}`;
  }

  // fallback: try matching slugged names in URL
  const urlLower = decodeURIComponent(String(url)).toLowerCase().replace(/_/g, "-");
  for (const [k, nm] of Object.entries(official)) {
    const name = cleanElectorateName(nm);
    const slug = name
      .toLowerCase()
      .replace(/[\u2019']/g, "")
      .replace(/\s+/g, "-");
    if (slug && urlLower.includes(slug)) {
      const n = Number(k);
      const nStr = Number.isFinite(n) ? String(n).padStart(3, "0") : "UNK";
      return `${nStr}_${safePart(name)}`;
    }
  }

  return null;
}

/* -------------------------
 *  A) META ENDPOINTS
 * ------------------------- */

// Overwrite meta for a term (dual structure)
app.post("/meta/electorates", (req, res) => {
  const { termKey, official_order, alphabetical_order } = req.body || {};
  if (!termKey || !official_order || !alphabetical_order) {
    return res.status(400).json({
      ok: false,
      error: "Expected { termKey, official_order, alphabetical_order }",
    });
  }

  // Clean names + ensure official_order is num->name
  const cleanedOfficial = {};
  for (const [num, name] of Object.entries(official_order)) {
    const n = Number(num);
    if (!Number.isFinite(n) || n <= 0) continue;
    const clean = cleanElectorateName(name);
    if (clean) cleanedOfficial[String(n)] = clean;
  }

  // Rebuild alphabetical_order from cleaned official (so it can’t be polluted)
  const names = Object.values(cleanedOfficial);
  const alpha = [...names].sort((a, b) => a.localeCompare(b, "en", { sensitivity: "base" }));
  const rebuiltAlpha = {};
  alpha.forEach((nm, i) => (rebuiltAlpha[nm] = i + 1));

  const meta = loadElectoratesMeta();
  meta[termKey] = { official_order: cleanedOfficial, alphabetical_order: rebuiltAlpha };
  saveElectoratesMeta(meta);

  res.json({ ok: true, termKey, count: Object.keys(cleanedOfficial).length });
});

app.get("/meta/electorates", (_req, res) => {
  res.json(loadElectoratesMeta());
});

// Optional: reset all electorate meta (handy if you want a clean rebuild)
app.post("/meta/electorates/reset", (_req, res) => {
  saveElectoratesMeta({});
  res.json({ ok: true });
});

/* -------------------------
 *  B) ARTIFACT ENDPOINTS
 * ------------------------- */

// Save a JSON artifact (urls-level-*.json, files-level-*.json, etc.)
app.post("/artifact/json", (req, res) => {
  const { artifact_name, payload } = req.body || {};
  if (!artifact_name || payload === undefined) {
    return res.status(400).json({ ok: false, error: "Expected { artifact_name, payload }" });
  }

  const outPath = path.join(ARTIFACT_DIR, safePart(artifact_name));
  writeJson(outPath, payload);
  res.json({ ok: true, saved_as: outPath });
});

// Append an item to JSONL log
app.post("/artifact/jsonl", (req, res) => {
  const { artifact_name, item } = req.body || {};
  if (!artifact_name || item === undefined) {
    return res.status(400).json({ ok: false, error: "Expected { artifact_name, item }" });
  }

  const outPath = path.join(ARTIFACT_DIR, safePart(artifact_name) + ".jsonl");
  appendJsonl(outPath, item);
  res.json({ ok: true, saved_as: outPath });
});

/* -------------------------
 *  C) FILE SINK ENDPOINT
 * ------------------------- */

// POST /sink (multipart form-data):
// - url: source URL
// - filename: optional (otherwise uses uploaded original filename)
// - file: binary
app.post("/sink", upload.single("file"), (req, res) => {
  const srcUrl = req.body?.url;
  if (!srcUrl) return res.status(400).json({ ok: false, error: "Missing form field: url" });
  if (!req.file?.buffer) return res.status(400).json({ ok: false, error: "Missing multipart file field: file" });

  const url = normalizeUrl(srcUrl);
  const termKey = termKeyForUrl(url);

  const meta = loadElectoratesMeta();

  const baseDir = termKey ? path.join(DOWNLOADS_ROOT, termKey) : path.join(DOWNLOADS_ROOT, "_unresolved");
  ensureDir(baseDir);

  const electorateFolder = termKey ? inferElectorateFolder(termKey, url, meta) : null;
  const outDir = electorateFolder ? path.join(baseDir, electorateFolder) : baseDir;
  ensureDir(outDir);

  const filename = safePart(req.body?.filename || req.file.originalname || "download.bin");
  const outPath = path.join(outDir, filename);

  // overwrite
  fs.writeFileSync(outPath, req.file.buffer);

  // optional per-file log
  appendJsonl(path.join(META_DIR, "file_saves.jsonl"), {
    ts: new Date().toISOString(),
    url,
    termKey: termKey || null,
    electorate: electorateFolder || null,
    saved_as: outPath,
    bytes: req.file.buffer.length,
  });

  res.json({ ok: true, saved_as: outPath, termKey: termKey || null, electorate: electorateFolder || null });
});

app.get("/health", (_req, res) => res.json({ ok: true }));

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`sink listening on http://localhost:${PORT}`);
  console.log(`DOWNLOADS_ROOT=${DOWNLOADS_ROOT}`);
});
