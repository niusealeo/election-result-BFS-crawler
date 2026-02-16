/**
 * sink.js — single server for:
 *  - saving downloaded files to term/electorate folders (overwrite)
 *  - persisting Postman runner JSON artifacts (urls-level / files-level) + run logs
 *  - electorate maps (dual structure per term)
 *  - RUN-SCOPED dedupe for Discover Links by level (reruns replace level contribution)
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
app.use(express.json({ limit: "250mb" }));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 350 * 1024 * 1024 }, // 350MB per file
});

const DOWNLOADS_ROOT = path.resolve(process.env.DOWNLOADS_ROOT || "./downloads");
const META_DIR = path.join(DOWNLOADS_ROOT, "_meta");
const ARTIFACT_DIR = path.join(META_DIR, "artifacts");
const ELECTORATES_META_PATH = path.join(META_DIR, "electorates_by_term.json");
const RUNS_DIR = path.join(META_DIR, "runs");

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
ensureDir(DOWNLOADS_ROOT);
ensureDir(META_DIR);
ensureDir(ARTIFACT_DIR);
ensureDir(RUNS_DIR);

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

// -------------------- Term mapping --------------------

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

// Event year -> parent GE year (extend as needed)
const YEAR_TO_PARENT_GE_YEAR = {
  2013: 2011, // referendum example
  2025: 2023, // by-election example
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

  // fallback bands
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

function electorateNumFromUrl(u) {
  let m = String(u).match(/electorate-details-(\d+)\.html/i);
  if (m) return Number(m[1]);

  m = String(u).match(/split-votes-electorate-(\d+)\.html/i);
  if (m) return Number(m[1]);

  m = String(u).match(/electorate-(\d+)[^\/]*\.html/i);
  if (m) return Number(m[1]);

  return null;
}

function cleanElectorateName(name) {
  return String(name || "")
    .replace(/\s*\(\.pdf[^)]*\)\s*$/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function loadElectoratesMeta() {
  return readJsonSafe(ELECTORATES_META_PATH, {});
}
function saveElectoratesMeta(meta) {
  writeJson(ELECTORATES_META_PATH, meta);
}

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

  // slug match fallback
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

// -------------------- A) Electorate meta endpoints --------------------

app.post("/meta/electorates", (req, res) => {
  const { termKey, official_order, alphabetical_order } = req.body || {};
  if (!termKey || !official_order || !alphabetical_order) {
    return res.status(400).json({ ok: false, error: "Expected { termKey, official_order, alphabetical_order }" });
  }

  const meta = loadElectoratesMeta();

  const cleanedOfficial = {};
  for (const [num, name] of Object.entries(official_order)) {
    const n = Number(num);
    if (!Number.isFinite(n) || n <= 0 || !Number.isInteger(n)) continue;
    const clean = cleanElectorateName(name);
    if (clean) cleanedOfficial[String(n)] = clean;
  }

  // rebuild alpha defensively
  const names = Object.values(cleanedOfficial);
  const alpha = [...names].sort((a, b) => a.localeCompare(b, "en", { sensitivity: "base" }));
  const rebuiltAlpha = {};
  alpha.forEach((nm, i) => (rebuiltAlpha[nm] = i + 1));

  meta[termKey] = { official_order: cleanedOfficial, alphabetical_order: rebuiltAlpha };
  saveElectoratesMeta(meta);

  res.json({ ok: true, termKey, count: Object.keys(cleanedOfficial).length });
});

app.get("/meta/electorates", (_req, res) => {
  res.json(loadElectoratesMeta());
});

app.post("/meta/electorates/reset", (_req, res) => {
  saveElectoratesMeta({});
  res.json({ ok: true });
});

// -------------------- B) Artifact endpoints --------------------

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

// Append JSONL log
app.post("/artifact/jsonl", (req, res) => {
  const { artifact_name, item } = req.body || {};
  if (!artifact_name || item === undefined) {
    return res.status(400).json({ ok: false, error: "Expected { artifact_name, item }" });
  }
  const outPath = path.join(ARTIFACT_DIR, safePart(artifact_name) + ".jsonl");
  appendJsonl(outPath, item);
  res.json({ ok: true, saved_as: outPath });
});

// -------------------- C) File sink endpoint --------------------

// POST /sink (multipart form-data):
// - url: source URL
// - filename: optional
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

// -------------------- D) RUN-SCOPED DEDUPE (Discover Links) --------------------
/**
 * POST /dedupe/level
 * Body: {
 *   run_id: "run_...." (required),
 *   level: 1..N (required),
 *   pages: [{url}, ...] (required),
 *   files: [{url, ext}, ...] (optional)
 * }
 *
 * Semantics:
 * - run-scoped state under: downloads/_meta/runs/<run_id_sanitized>/
 * - each level rerun replaces its own contribution:
 *     remove old level contribution from seen sets
 *     filter new candidate lists against remaining seen (previous levels)
 *     store new level contribution
 * - writes:
 *     artifacts/urls-level-(level+1).json   (new pages only)
 *     artifacts/files-level-(level).json   (new files only; meta row includes ext:"meta")
 */

function runPaths(runIdRaw) {
  const runTag = safePart(runIdRaw);
  const runDir = path.join(RUNS_DIR, runTag);
  ensureDir(runDir);
  return {
    runTag,
    runDir,
    seenPagesPath: path.join(runDir, "seen_pages.json"),
    seenFilesPath: path.join(runDir, "seen_files.json"),
  };
}

// Helper: list all levels we have stored for this run (from level_pages_*.json files)
function listStoredLevels(runDir) {
  const levels = new Set();
  for (const f of fs.readdirSync(runDir)) {
    const m = f.match(/^level_pages_(\d+)\.json$/);
    if (m) levels.add(Number(m[1]));
  }
  return Array.from(levels).sort((a, b) => a - b);
}

app.post("/dedupe/level", (req, res) => {
  const { run_id, level, pages, files } = req.body || {};
  if (!run_id) return res.status(400).json({ ok: false, error: "Missing run_id" });

  const L = Number(level);
  if (!Number.isFinite(L) || L < 1) {
    return res.status(400).json({ ok: false, error: "Invalid level" });
  }
  if (!Array.isArray(pages)) {
    return res.status(400).json({ ok: false, error: "pages must be an array of {url}" });
  }

  const { runDir, seenPagesPath, seenFilesPath, runTag } = runPaths(run_id);

  // Load current seen memory (union of all previously stored levels in this run)
  const seenPages = new Set(readJsonSafe(seenPagesPath, []));
  const seenFiles = new Set(readJsonSafe(seenFilesPath, []));

  // STRICT BFS POLICY A:
  // Remove contributions for ALL levels >= L (so rerun of level L isn't blocked by stale downstream)
  const storedLevels = listStoredLevels(runDir);
  const toClear = storedLevels.filter(x => x >= L);

  for (const lvl of toClear) {
    const levelPagesPath = path.join(runDir, `level_pages_${lvl}.json`);
    const levelFilesPath = path.join(runDir, `level_files_${lvl}.json`);

    const oldPages = readJsonSafe(levelPagesPath, []);
    const oldFiles = readJsonSafe(levelFilesPath, []);

    for (const u of oldPages) seenPages.delete(u);
    for (const u of oldFiles) seenFiles.delete(u);

    // Remove the stored level contribution files (they're now invalid)
    if (fs.existsSync(levelPagesPath)) fs.unlinkSync(levelPagesPath);
    if (fs.existsSync(levelFilesPath)) fs.unlinkSync(levelFilesPath);
  }

  // Now seenPages/seenFiles contains only memory of levels < L.

  // Filter incoming candidates against remaining memory
  const newPages = [];
  for (const row of pages) {
    const u = row?.url;
    if (!u) continue;
    if (seenPages.has(u)) continue;
    seenPages.add(u);
    newPages.push(u);
  }

  const inFiles = Array.isArray(files) ? files : [];
  const newFiles = [];
  for (const row of inFiles) {
    const u = row?.url;
    if (!u) continue;
    if (seenFiles.has(u)) continue;
    seenFiles.add(u);
    newFiles.push({ url: u, ext: row?.ext || "unknown" });
  }

  // Persist this level's new contribution (for future subtract)
  writeJson(path.join(runDir, `level_pages_${L}.json`), newPages);
  writeJson(path.join(runDir, `level_files_${L}.json`), newFiles.map(f => f.url));

  // Persist updated run memory
  writeJson(seenPagesPath, Array.from(seenPages));
  writeJson(seenFilesPath, Array.from(seenFiles));

  // Write artifacts (overwrite)
  const nextLevel = L + 1;

  // urls-level-(L+1).json : conflate meta with first real row (no extra header)
  const outPages = newPages.map(url => ({ url }));
  if (outPages.length > 0) {
    outPages[0] = { _meta: true, level: nextLevel, kind: "urls", ...outPages[0] };
  }
  writeJson(path.join(ARTIFACT_DIR, `urls-level-${nextLevel}.json`), outPages);

  // files-level-L.json : conflate meta with first real row (no duplicate); only if any files exist
  if (newFiles.length > 0) {
    const outFiles = newFiles.map(f => ({ url: f.url, ext: f.ext || "unknown" }));
    outFiles[0] = { _meta: true, level: L, kind: "files", ...outFiles[0] };
    writeJson(path.join(ARTIFACT_DIR, `files-level-${L}.json`), outFiles);
  } else {
    const fp = path.join(ARTIFACT_DIR, `files-level-${L}.json`);
    if (fs.existsSync(fp)) fs.unlinkSync(fp);
  }

  appendJsonl(path.join(runDir, "dedupe_log.jsonl"), {
    ts: new Date().toISOString(),
    run_id: runTag,
    level: L,
    cleared_levels_ge: toClear,
    pages_new: newPages.length,
    files_new: newFiles.length
  });

  res.json({
    ok: true,
    run_id: runTag,
    level: L,
    cleared_levels_ge: toClear,
    pages_new: newPages.length,
    files_new: newFiles.length,
    saved_pages: `urls-level-${nextLevel}.json`,
    saved_files: newFiles.length > 0 ? `files-level-${L}.json` : null
  });
});

// Reset a run’s dedupe memory (optional)
app.post("/dedupe/reset", (req, res) => {
  const { run_id } = req.body || {};
  if (!run_id) return res.status(400).json({ ok: false, error: "Missing run_id" });

  const { runDir, seenPagesPath, seenFilesPath } = runPaths(run_id);
  writeJson(seenPagesPath, []);
  writeJson(seenFilesPath, []);

  // Remove stored per-level contributions too
  for (const f of fs.readdirSync(runDir)) {
    if (/^level_pages_\d+\.json$/.test(f) || /^level_files_\d+\.json$/.test(f)) {
      fs.unlinkSync(path.join(runDir, f));
    }
  }

  res.json({ ok: true, run_id: safePart(run_id) });
});

// -------------------- Health --------------------
app.get("/health", (_req, res) => res.json({ ok: true }));

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`sink listening on http://localhost:${PORT}`);
  console.log(`DOWNLOADS_ROOT=${DOWNLOADS_ROOT}`);
});
