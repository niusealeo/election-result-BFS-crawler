/**
 * sink.js — single BFS crawl + file sink
 *
 * Usage:
 *   npm i express multer sanitize-filename
 *   node sink.js
 *
 * Writes to:
 *   ./downloads/
 *     term_.../ ... (downloaded files saved via /sink/raw)
 *   ./downloads/_meta/runs/BFS_crawl/
 *     seen_pages.json, seen_files.json
 *     level_pages_N.json, level_files_N.json
 *     dedupe_log.jsonl
 *   ./downloads/_meta/artifacts/
 *     urls-level-N.json, files-level-N.json
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const sanitize = require("sanitize-filename");

const app = express();
app.use(express.json({ limit: "750mb" })); // allow big base64 payloads

// ---------- paths ----------
const DOWNLOADS_ROOT = path.resolve(process.env.DOWNLOADS_ROOT || "./downloads");
const META_DIR = path.join(DOWNLOADS_ROOT, "_meta");
const ARTIFACT_DIR = path.join(META_DIR, "artifacts");
const RUNS_DIR = path.join(META_DIR, "runs");

// one shared crawl folder
const BFS_RUN_TAG = "BFS_crawl";
const BFS_RUN_DIR = path.join(RUNS_DIR, BFS_RUN_TAG);

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
ensureDir(DOWNLOADS_ROOT);
ensureDir(META_DIR);
ensureDir(ARTIFACT_DIR);
ensureDir(RUNS_DIR);
ensureDir(BFS_RUN_DIR);

function readJsonSafe(p, fallback) {
  try { return JSON.parse(fs.readFileSync(p, "utf8")); }
  catch { return fallback; }
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

// ---------- electorate/term routing (minimal; extend if needed) ----------
const TERM_BY_GE_YEAR = {
  1996: 45, 1999: 46, 2002: 47, 2005: 48, 2008: 49,
  2011: 50, 2014: 51, 2017: 52, 2020: 53, 2023: 54,
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
  if (y != null) {
    // coarse fallback bands
    if (y >= 2023) return "term_54_(2023)";
    if (y >= 2020) return "term_53_(2020)";
    if (y >= 2017) return "term_52_(2017)";
    if (y >= 2014) return "term_51_(2014)";
    if (y >= 2011) return "term_50_(2011)";
    if (y >= 2008) return "term_49_(2008)";
    if (y >= 2005) return "term_48_(2005)";
    if (y >= 2002) return "term_47_(2002)";
    if (y >= 1999) return "term_46_(1999)";
    if (y >= 1996) return "term_45_(1996)";
  }
  return null;
}

// ---------- health ----------
app.get("/health", (_req, res) => res.json({ ok: true }));

// ---------- BFS state paths ----------
function bfsPaths() {
  return {
    runTag: BFS_RUN_TAG,
    runDir: BFS_RUN_DIR,
    seenPagesPath: path.join(BFS_RUN_DIR, "seen_pages.json"),
    seenFilesPath: path.join(BFS_RUN_DIR, "seen_files.json"),
  };
}

function listStoredLevels(runDir) {
  const levels = new Set();
  for (const f of fs.readdirSync(runDir)) {
    const m = f.match(/^level_pages_(\d+)\.json$/);
    if (m) levels.add(Number(m[1]));
  }
  return Array.from(levels).sort((a, b) => a - b);
}

// ---------- DEDUPE: Policy A (rerun level L clears levels >= L) ----------
app.post("/dedupe/level", (req, res) => {
  const { level, pages, files } = req.body || {};

  const L = Number(level);
  if (!Number.isFinite(L) || L < 1) {
    return res.status(400).json({ ok: false, error: "Invalid level" });
  }
  if (!Array.isArray(pages)) {
    return res.status(400).json({ ok: false, error: "pages must be an array of {url}" });
  }

  const { runDir, seenPagesPath, seenFilesPath, runTag } = bfsPaths();

  const seenPages = new Set(readJsonSafe(seenPagesPath, []));
  const seenFiles = new Set(readJsonSafe(seenFilesPath, []));

  // POLICY A: clear contributions for all levels >= L
  const storedLevels = listStoredLevels(runDir);
  const toClear = storedLevels.filter(x => x >= L);

  for (const lvl of toClear) {
    const lp = path.join(runDir, `level_pages_${lvl}.json`);
    const lf = path.join(runDir, `level_files_${lvl}.json`);

    const oldPages = readJsonSafe(lp, []);
    const oldFiles = readJsonSafe(lf, []);

    for (const u of oldPages) seenPages.delete(u);
    for (const u of oldFiles) seenFiles.delete(u);

    if (fs.existsSync(lp)) fs.unlinkSync(lp);
    if (fs.existsSync(lf)) fs.unlinkSync(lf);
  }

  // Filter incoming against memory of levels < L
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

  // persist level contribution
  writeJson(path.join(runDir, `level_pages_${L}.json`), newPages);
  writeJson(path.join(runDir, `level_files_${L}.json`), newFiles.map(f => f.url));

  // persist union memory
  writeJson(seenPagesPath, Array.from(seenPages));
  writeJson(seenFilesPath, Array.from(seenFiles));

  // write artifacts, conflating meta with the FIRST REAL ROW (no duplicate row)
  const nextLevel = L + 1;

  const outPages = newPages.map(url => ({ url }));
  if (outPages.length > 0) {
    outPages[0] = { _meta: true, level: nextLevel, kind: "urls", ...outPages[0] };
  }
  writeJson(path.join(ARTIFACT_DIR, `urls-level-${nextLevel}.json`), outPages);

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
    run: runTag,
    level: L,
    cleared_levels_ge: toClear,
    pages_new: newPages.length,
    files_new: newFiles.length
  });

  res.json({
    ok: true,
    run: runTag,
    level: L,
    cleared_levels_ge: toClear,
    pages_new: newPages.length,
    files_new: newFiles.length,
    saved_pages: `urls-level-${nextLevel}.json`,
    saved_files: newFiles.length > 0 ? `files-level-${L}.json` : null
  });
});

// wipe everything (optional)
app.post("/dedupe/reset", (_req, res) => {
  const { runDir, seenPagesPath, seenFilesPath } = bfsPaths();
  writeJson(seenPagesPath, []);
  writeJson(seenFilesPath, []);

  for (const f of fs.readdirSync(runDir)) {
    if (/^level_pages_\d+\.json$/.test(f) || /^level_files_\d+\.json$/.test(f) || f.endsWith(".jsonl")) {
      fs.unlinkSync(path.join(runDir, f));
    }
  }
  res.json({ ok: true, run: BFS_RUN_TAG });
});

// ---------- FILE SINK (raw base64; overwrites existing) ----------
/**
 * POST /sink/raw
 * Body: { url, filename?, b64 }
 *
 * Saves to downloads/<termKey>/[filename] (overwrite).
 * (You can extend routing for electorates later; this is the stable baseline.)
 */
app.post("/sink/raw", (req, res) => {
  const { url, filename, b64 } = req.body || {};
  if (!url) return res.status(400).json({ ok: false, error: "Missing url" });
  if (!b64) return res.status(400).json({ ok: false, error: "Missing b64" });

  let buf;
  try {
    buf = Buffer.from(String(b64), "base64");
  } catch {
    return res.status(400).json({ ok: false, error: "Invalid base64" });
  }

  const termKey = termKeyForUrl(url) || "_unresolved";
  const outDir = path.join(DOWNLOADS_ROOT, termKey);
  ensureDir(outDir);

  const fname = safePart(filename || path.basename(new URL(url).pathname) || "download.bin");
  const outPath = path.join(outDir, fname);

  fs.writeFileSync(outPath, buf);

  appendJsonl(path.join(META_DIR, "file_saves.jsonl"), {
    ts: new Date().toISOString(),
    url,
    termKey,
    saved_as: outPath,
    bytes: buf.length
  });

  res.json({ ok: true, saved_as: outPath, termKey, bytes: buf.length });
});

// ---------- (optional) save runner artifacts directly ----------
app.post("/artifact/json", (req, res) => {
  const { artifact_name, payload } = req.body || {};
  if (!artifact_name || payload === undefined) {
    return res.status(400).json({ ok: false, error: "Expected { artifact_name, payload }" });
  }
  const outPath = path.join(ARTIFACT_DIR, safePart(artifact_name));
  writeJson(outPath, payload);
  res.json({ ok: true, saved_as: outPath });
});

app.post("/artifact/jsonl", (req, res) => {
  const { artifact_name, item } = req.body || {};
  if (!artifact_name || item === undefined) {
    return res.status(400).json({ ok: false, error: "Expected { artifact_name, item }" });
  }
  const outPath = path.join(ARTIFACT_DIR, safePart(artifact_name) + ".jsonl");
  appendJsonl(outPath, item);
  res.json({ ok: true, saved_as: outPath });
});

// ---------- start ----------
const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`sink listening on http://localhost:${PORT}`);
  console.log(`DOWNLOADS_ROOT=${DOWNLOADS_ROOT}`);
  console.log(`BFS state folder: ${BFS_RUN_DIR}`);
});
