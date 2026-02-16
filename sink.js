/**
 * sink.js — One big BFS crawl state, no IDs.
 *
 * Folders:
 *   ./BFS_crawl/
 *      runs/                 (logs)
 *      _meta/                (state + artifacts + electorates_by_term.json)
 *   ./downloads/             (SIBLING of BFS_crawl; actual downloaded files)
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const { URL } = require("url");

// ----------------------- config / paths -----------------------
const PORT = Number(process.env.PORT || 3000);

const BFS_ROOT = path.resolve(process.cwd(), "BFS_crawl");

// downloads are sibling of BFS_crawl (NOT a child)
const DOWNLOADS_ROOT = path.resolve(process.cwd(), "downloads");

// logs in BFS_crawl/runs
const RUNS_DIR = path.join(BFS_ROOT, "runs");

// meta in BFS_crawl/_meta
const META_DIR = path.join(BFS_ROOT, "_meta");
const ARTIFACT_DIR = path.join(META_DIR, "artifacts");
const STATE_PATH = path.join(META_DIR, "state.json");
const ELECTORATES_BY_TERM_PATH = path.join(META_DIR, "electorates_by_term.json");

// ----------------------- helpers -----------------------
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function readJsonSafe(p, fallback) {
  try {
    if (!fs.existsSync(p)) return fallback;
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
function unlinkIfExists(p) {
  try { if (fs.existsSync(p)) fs.unlinkSync(p); } catch {}
}

function normalizeUrl(u) {
  try {
    const U = new URL(String(u).trim());
    // remove hash; keep query (some endpoints rely on it)
    U.hash = "";
    // normalize /index.html to /
    if (U.pathname.endsWith("/index.html")) U.pathname = U.pathname.replace(/\/index\.html$/, "/");
    // collapse // in path
    U.pathname = U.pathname.replace(/\/{2,}/g, "/");
    return U.toString();
  } catch {
    return String(u || "").trim();
  }
}

function extFromUrl(u) {
  const m = String(u).match(/\.([a-z0-9]+)(?:\?|#|$)/i);
  return m ? m[1].toLowerCase() : "bin";
}

function safeFilename(name) {
  // minimal safe: remove path separators + control chars
  return String(name || "download.bin")
    .replace(/[\/\\]/g, "_")
    .replace(/[\u0000-\u001f]/g, "")
    .slice(0, 240) || "download.bin";
}

function filenameFromUrl(u) {
  try {
    const U = new URL(u);
    const base = path.basename(U.pathname);
    return safeFilename(base || "download.bin");
  } catch {
    return "download.bin";
  }
}

function asciiFold(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function termKeyParts(termKey) {
  const m = String(termKey).match(/^term_(\d+)_\((\d{4})\)$/);
  if (!m) return null;
  return { termNo: Number(m[1]), geYear: Number(m[2]) };
}

function stableUniq(arr) {
  const seen = new Set();
  const out = [];
  for (const x of arr) {
    if (!seen.has(x)) { seen.add(x); out.push(x); }
  }
  return out;
}

// ----------------------- init folders -----------------------
ensureDir(BFS_ROOT);
ensureDir(RUNS_DIR);
ensureDir(META_DIR);
ensureDir(ARTIFACT_DIR);
ensureDir(DOWNLOADS_ROOT);

// logs
const DEDUPE_LOG = path.join(RUNS_DIR, "dedupe_log.jsonl");
const FILE_SAVES_LOG = path.join(RUNS_DIR, "file_saves.jsonl");
const ELECTORATES_INGEST_LOG = path.join(RUNS_DIR, "electorates_ingest.jsonl");

// ----------------------- BFS state model -----------------------
/**
 * state.json:
 * {
 *   levels: {
 *     "1": { visited: [url...], pages: [url...], files: [{url, ext}...] },
 *     "2": ...
 *   }
 * }
 */
function defaultState() {
  return { levels: {} };
}
function loadState() {
  return readJsonSafe(STATE_PATH, defaultState());
}
function saveState(st) {
  writeJson(STATE_PATH, st);
}

function computeSeenUpTo(st, maxLevelInclusive) {
  const seenPages = new Set();
  const seenFiles = new Set();

  const levels = Object.keys(st.levels)
    .map(k => Number(k))
    .filter(n => Number.isFinite(n))
    .sort((a, b) => a - b);

  for (const L of levels) {
    if (L > maxLevelInclusive) break;
    const rec = st.levels[String(L)];
    if (!rec) continue;
    for (const u of (rec.visited || [])) seenPages.add(normalizeUrl(u));
    for (const u of (rec.pages || [])) seenPages.add(normalizeUrl(u));
    for (const f of (rec.files || [])) if (f?.url) seenFiles.add(normalizeUrl(f.url));
  }
  return { seenPages, seenFiles };
}

// ----------------------- electorates map + routing -----------------------
function loadElectoratesByTerm() {
  return readJsonSafe(ELECTORATES_BY_TERM_PATH, {});
}

function termKeyForUrl(u, electoratesByTerm) {
  const url = String(u);

  // GE archive URL
  let m = url.match(/\/electionresults_(\d{4})\//i);
  if (m) {
    const geYear = Number(m[1]);
    // find exact term key by GE year
    for (const k of Object.keys(electoratesByTerm)) {
      const p = termKeyParts(k);
      if (p && p.geYear === geYear) return k;
    }
  }

  // by-election / referendum style includes event year
  m = url.match(/\/(\d{4})_[^/]*(byelection|by-election|referendum)\//i);
  let eventYear = m ? Number(m[1]) : null;
  if (!eventYear) {
    // fallback: first year in path
    m = url.match(/(19\d{2}|20\d{2})/);
    if (m) eventYear = Number(m[1]);
  }
  if (!eventYear) return "term_unknown";

  // choose term where term.geYear <= eventYear < nextTerm.geYear
  const terms = Object.keys(electoratesByTerm)
    .map(k => ({ k, p: termKeyParts(k) }))
    .filter(x => x.p)
    .sort((a, b) => a.p.geYear - b.p.geYear);

  for (let i = 0; i < terms.length; i++) {
    const cur = terms[i];
    const next = terms[i + 1];
    if (!next) return cur.k;
    if (eventYear >= cur.p.geYear && eventYear < next.p.geYear) return cur.k;
  }
  return terms[terms.length - 1]?.k || "term_unknown";
}

function electorateFolderFor(termKey, url, electoratesByTerm) {
  const t = electoratesByTerm[termKey];
  if (!t?.official_order) return null;

  const u = String(url);

  // Pattern /eNN/
  let m = u.match(/\/e(\d{1,3})\//i);
  if (m) {
    const n = Number(m[1]);
    const name = t.official_order[String(n)];
    if (name) return `${String(n).padStart(3, "0")}_${name}`;
  }

  // Pattern /YYYY_slug_byelection/
  m = u.match(/\/\d{4}_([^/]+?)_(?:byelection|by-election)\//i);
  if (m) {
    const guess = asciiFold(m[1].replace(/[_-]+/g, " "));
    for (const [numStr, name] of Object.entries(t.official_order)) {
      if (asciiFold(name) === guess) {
        return `${String(Number(numStr)).padStart(3, "0")}_${name}`;
      }
    }
  }

  // Fallback: match electorate name tokens in URL (folded)
  const foldedUrl = asciiFold(u.replace(/[^a-z0-9]+/g, " "));
  for (const [numStr, name] of Object.entries(t.official_order)) {
    const foldedName = asciiFold(name);
    if (
      foldedUrl.includes(` ${foldedName} `) ||
      foldedUrl.startsWith(`${foldedName} `) ||
      foldedUrl.endsWith(` ${foldedName}`)
    ) {
      return `${String(Number(numStr)).padStart(3, "0")}_${name}`;
    }
  }

  return null;
}

// ----------------------- app -----------------------
const app = express();
app.use(express.json({ limit: "750mb" }));

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    BFS_ROOT,
    DOWNLOADS_ROOT,
    META_DIR,
    RUNS_DIR
  });
});

// ---------- 1) DEDUPE LEVEL ----------
// DOES NOT WRITE EMPTY urls-level-(L+1).json
app.post("/dedupe/level", (req, res) => {
  try {
    const level = Number(req.body?.level);
    if (!Number.isFinite(level) || level < 1) {
      return res.status(400).json({ ok: false, error: "Invalid level" });
    }

    const toUrlArray = (raw) =>
      stableUniq(
        (Array.isArray(raw) ? raw : [])
          .map(r => (typeof r === "string" ? r : r?.url))
          .filter(Boolean)
          .map(normalizeUrl)
      );

    const visited = toUrlArray(req.body?.visited || []);
    const pages = toUrlArray(req.body?.pages || []);

    const inFiles = Array.isArray(req.body?.files) ? req.body.files : [];
    const files = stableUniq(
      inFiles
        .filter(f => f?.url)
        .map(f => ({
          url: normalizeUrl(f.url),
          ext: (f.ext || extFromUrl(f.url) || "bin").toLowerCase()
        }))
        .map(f => JSON.stringify(f))
    ).map(s => JSON.parse(s));

    // Load + replace level record (rerun-safe)
    const st = loadState();
    st.levels[String(level)] = { visited, pages, files };
    saveState(st);

    // Build seen sets for filtering:
    // - Next level must exclude anything seen in levels < level
    // - Also exclude current level's visited seeds (so seed won't reappear)
    const { seenPages: seenPagesBefore, seenFiles: seenFilesBefore } = computeSeenUpTo(st, level - 1);
    const seenForNextPages = new Set([...seenPagesBefore, ...visited]);
    const nextPages = pages.filter(u => !seenForNextPages.has(u));

    const filesOut = files.filter(f => !seenFilesBefore.has(f.url));

    // Write artifacts into BFS_crawl/_meta/artifacts
    const nextLevel = level + 1;
    const nextUrlsPath = path.join(ARTIFACT_DIR, `urls-level-${nextLevel}.json`);
    const filesPath = path.join(ARTIFACT_DIR, `files-level-${level}.json`);

    if (nextPages.length > 0) {
      // per-row carries level/kind; no separate meta row
      writeJson(nextUrlsPath, nextPages.map(url => ({ url, level: nextLevel, kind: "urls" })));
    } else {
      // IMPORTANT: do not produce an empty file
      unlinkIfExists(nextUrlsPath);
    }

    if (filesOut.length > 0) {
      writeJson(filesPath, filesOut.map(f => ({ url: f.url, ext: f.ext, level, kind: "files" })));
    } else {
      unlinkIfExists(filesPath);
    }

    appendJsonl(DEDUPE_LOG, {
      ts: new Date().toISOString(),
      level,
      visited: visited.length,
      pages_in: pages.length,
      pages_out_next: nextPages.length,
      files_in: files.length,
      files_out: filesOut.length
    });

    res.json({
      ok: true,
      level,
      next_level: nextLevel,
      wrote_next_urls: nextPages.length,
      wrote_files: filesOut.length,
      next_urls_path: nextPages.length ? nextUrlsPath : null,
      files_path: filesOut.length ? filesPath : null
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- 2) UPLOAD FILE (save into term/electorate structure) ----------
app.post("/upload/file", (req, res) => {
  try {
    const url = normalizeUrl(req.body?.url);
    const ext = (req.body?.ext || extFromUrl(url) || "bin").toLowerCase();
    const b64 = req.body?.content_base64;

    if (!url || !b64) return res.status(400).json({ ok: false, error: "Missing url or content_base64" });

    const electoratesByTerm = loadElectoratesByTerm();
    const termKey = termKeyForUrl(url, electoratesByTerm);
    const electorateFolder = electorateFolderFor(termKey, url, electoratesByTerm);

    const termDir = path.join(DOWNLOADS_ROOT, termKey);
    const finalDir = electorateFolder ? path.join(termDir, electorateFolder) : termDir;
    ensureDir(finalDir);

    let filename = filenameFromUrl(url);
    if (!/\.[a-z0-9]+$/i.test(filename) && ext) filename += `.${ext}`;
    filename = safeFilename(filename);

    const outPath = path.join(finalDir, filename);
    const buf = Buffer.from(String(b64), "base64");
    fs.writeFileSync(outPath, buf); // overwrite

    appendJsonl(FILE_SAVES_LOG, {
      ts: new Date().toISOString(),
      url,
      termKey,
      electorateFolder: electorateFolder || null,
      saved_to: outPath,
      bytes: buf.length
    });

    res.json({ ok: true, saved_to: outPath, bytes: buf.length });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- 3) ELECTORATES INGEST (build electorates_by_term.json from Postman) ----------
/**
 * POST /meta/electorates/upsert
 * Body:
 * {
 *   term_key: "term_54_(2023)",
 *   ge_year: 2023,              // optional, informational
 *   official_order: { "1": "Te Tai Tokerau", ... },     // REQUIRED
 *   alpha_index: { "Auckland Central": 1, ... }         // REQUIRED
 * }
 *
 * This writes/updates BFS_crawl/_meta/electorates_by_term.json
 */
app.post("/meta/electorates/upsert", (req, res) => {
  try {
    const termKey = String(req.body?.term_key || "").trim();
    if (!termKey) return res.status(400).json({ ok: false, error: "Missing term_key" });

    const official_order = req.body?.official_order;
    const alpha_index = req.body?.alpha_index;

    if (!official_order || typeof official_order !== "object") {
      return res.status(400).json({ ok: false, error: "Missing official_order object" });
    }
    if (!alpha_index || typeof alpha_index !== "object") {
      return res.status(400).json({ ok: false, error: "Missing alpha_index object" });
    }

    // basic sanity: official_order keys should be numeric strings
    const nums = Object.keys(official_order).filter(k => /^\d+$/.test(k)).map(k => Number(k));
    if (nums.length === 0) {
      return res.status(400).json({ ok: false, error: "official_order must have numeric string keys" });
    }

    const db = loadElectoratesByTerm();
    db[termKey] = {
      term_key: termKey,
      ge_year: req.body?.ge_year ?? null,
      official_order,
      alpha_index
    };
    writeJson(ELECTORATES_BY_TERM_PATH, db);

    appendJsonl(ELECTORATES_INGEST_LOG, {
      ts: new Date().toISOString(),
      term_key: termKey,
      count: Object.keys(official_order).length
    });

    res.json({ ok: true, saved: ELECTORATES_BY_TERM_PATH, term_key: termKey });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.listen(PORT, () => {
  console.log(`sink listening on http://localhost:${PORT}`);
  console.log(`BFS_ROOT: ${BFS_ROOT}`);
  console.log(`DOWNLOADS_ROOT (sibling): ${DOWNLOADS_ROOT}`);
  console.log(`META_DIR: ${META_DIR}`);
  console.log(`RUNS_DIR: ${RUNS_DIR}`);
  console.log(`electorates_by_term.json: ${ELECTORATES_BY_TERM_PATH}`);
});
