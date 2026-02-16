/**
 * sink.js
 *
 * Single-crawl folder: BFS_crawl/
 * - Saves BFS level artifacts: BFS_crawl/levels/urls-level-N.json and files-level-N.json
 * - Maintains cross-run state in BFS_crawl/state.json (so multiple Postman runs continue BFS)
 * - Dedupe logic:
 *    * Each level is "replace-on-rerun"
 *    * New levels are filtered against all previous levels + visited inputs (including seeds)
 * - Does NOT write empty urls-level-(N+1).json when no new pages remain
 * - Saves downloaded files into directory structure:
 *    term_##_(GEYEAR)/[NNN_ElectorateName]/<filename>
 */

const express = require("express");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

// ---------- config ----------
const PORT = process.env.PORT || 3000;

// Change if you want a different root
const CRAWL_ROOT = path.resolve(process.cwd(), "BFS_crawl");
const LEVELS_DIR = path.join(CRAWL_ROOT, "levels");
const DOWNLOADS_DIR = path.join(CRAWL_ROOT, "downloads");
const STATE_PATH = path.join(CRAWL_ROOT, "state.json");

// Electorate mapping json produced earlier
const ELECTORATES_BY_TERM_PATH = path.resolve(process.cwd(), "electorates_by_term.json");

// ---------- helpers ----------
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function readJsonIfExists(p, fallback) {
  try {
    if (!fs.existsSync(p)) return fallback;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (e) {
    return fallback;
  }
}

function writeJson(p, obj) {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

function unlinkIfExists(p) {
  try {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch (_) {}
}

function stableUniq(arr) {
  const seen = new Set();
  const out = [];
  for (const x of arr) {
    if (!seen.has(x)) {
      seen.add(x);
      out.push(x);
    }
  }
  return out;
}

function normalizeUrl(u) {
  // Trim + remove obvious dot segments normalization via URL
  try {
    const uu = new URL(u);
    // Keep query (some election files rely on it), but normalize pathname
    uu.pathname = uu.pathname.replace(/\/\.\//g, "/").replace(/\/{2,}/g, "/");
    return uu.toString();
  } catch {
    return String(u || "").trim();
  }
}

function isTruthyMetaRow(row) {
  return row && (row._meta === true || row._meta === "true");
}

function getExtFromUrl(u) {
  const m = String(u).match(/\.([a-z0-9]+)(?:\?|#|$)/i);
  return m ? m[1].toLowerCase() : null;
}

function asciiFold(s) {
  // remove macrons/diacritics for matching URL slugs
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function slugToNameGuess(slug) {
  // "tamaki_makaurau" -> "tamaki makaurau" (folded)
  return asciiFold(String(slug || "").replace(/[_-]+/g, " ").trim());
}

function safeFileNameFromUrl(u) {
  try {
    const uu = new URL(u);
    const base = path.basename(uu.pathname);
    return base || "download.bin";
  } catch {
    return "download.bin";
  }
}

// ---------- load electorates map ----------
let electoratesByTerm = {};
try {
  electoratesByTerm = JSON.parse(fs.readFileSync(ELECTORATES_BY_TERM_PATH, "utf8"));
} catch (e) {
  console.warn("WARNING: electorates_by_term.json not found or invalid at:", ELECTORATES_BY_TERM_PATH);
  electoratesByTerm = {};
}

// parse term keys -> { termKey, termNo, geYear }
function parseTermKey(termKey) {
  // expects: "term_54_(2023)"
  const m = String(termKey).match(/^term_(\d+)_\((\d{4})\)$/);
  if (!m) return null;
  return { termKey, termNo: Number(m[1]), geYear: Number(m[2]) };
}

const termInfos = Object.keys(electoratesByTerm)
  .map(parseTermKey)
  .filter(Boolean)
  .sort((a, b) => a.geYear - b.geYear);

// find termKey for a year that occurs during a parliamentary term:
// term.geYear <= year < nextTerm.geYear (or last term if no next)
function termKeyForYear(eventYear) {
  const y = Number(eventYear);
  if (!Number.isFinite(y) || termInfos.length === 0) return null;

  for (let i = 0; i < termInfos.length; i++) {
    const cur = termInfos[i];
    const next = termInfos[i + 1];
    if (!next) return cur.termKey;
    if (y >= cur.geYear && y < next.geYear) return cur.termKey;
  }
  return termInfos[termInfos.length - 1]?.termKey || null;
}

function termKeyForGeYear(geYear) {
  const y = Number(geYear);
  const found = termInfos.find(t => t.geYear === y);
  return found ? found.termKey : null;
}

function electorateFolderFor(termKey, electorateNoOrName) {
  const t = electoratesByTerm[termKey];
  if (!t || !t.official_order) return null;

  // If numeric
  if (typeof electorateNoOrName === "number" || /^\d+$/.test(String(electorateNoOrName))) {
    const n = Number(electorateNoOrName);
    const name = t.official_order[String(n)];
    if (!name) return null;
    return `${String(n).padStart(3, "0")}_${name}`;
  }

  // If name
  const wanted = asciiFold(electorateNoOrName);
  for (const [numStr, name] of Object.entries(t.official_order)) {
    if (asciiFold(name) === wanted) {
      return `${String(Number(numStr)).padStart(3, "0")}_${name}`;
    }
  }
  return null;
}

function inferPlacementFromUrl(fileUrl) {
  const u = normalizeUrl(fileUrl);

  // Determine term
  let termKey = null;

  // A) general election archive style: /electionresults_YYYY/
  {
    const m = u.match(/\/electionresults_(\d{4})\//i);
    if (m) {
      termKey = termKeyForGeYear(Number(m[1]));
    }
  }

  // B) event style: /YYYY_*_byelection/ or /YYYY_*referendum/
  if (!termKey) {
    const m = u.match(/\/(\d{4})_[^/]*(byelection|by-election|referendum)/i);
    if (m) {
      termKey = termKeyForYear(Number(m[1]));
    }
  }

  // C) fallback: any year in path
  if (!termKey) {
    const m = u.match(/\/(\d{4})(?:[^0-9]|$)/);
    if (m) {
      termKey = termKeyForYear(Number(m[1]));
    }
  }

  // Determine electorate (optional)
  let electorateFolder = null;

  // 1) /eNN/ pattern like /e9/ (GE electorate index)
  if (termKey && !electorateFolder) {
    const m = u.match(/\/e(\d{1,3})\//i);
    if (m) electorateFolder = electorateFolderFor(termKey, Number(m[1]));
  }

  // 2) by-election slug: /2025_tamaki_makaurau_byelection/
  if (termKey && !electorateFolder) {
    const m = u.match(/\/\d{4}_([^/]+?)_(?:byelection|by-election)\//i);
    if (m) {
      const guess = slugToNameGuess(m[1]); // folded
      // match against official_order names
      const t = electoratesByTerm[termKey];
      if (t?.official_order) {
        for (const [numStr, name] of Object.entries(t.official_order)) {
          if (asciiFold(name) === guess) {
            electorateFolder = `${String(Number(numStr)).padStart(3, "0")}_${name}`;
            break;
          }
        }
      }
    }
  }

  // 3) try to match any electorate name appearing in URL (best-effort)
  if (termKey && !electorateFolder) {
    const t = electoratesByTerm[termKey];
    if (t?.official_order) {
      const foldedUrl = asciiFold(u.replace(/[^a-z0-9]+/g, " "));
      for (const [numStr, name] of Object.entries(t.official_order)) {
        const foldedName = asciiFold(name);
        // require whole-word-ish match to reduce false positives
        if (foldedUrl.includes(` ${foldedName} `) || foldedUrl.startsWith(`${foldedName} `) || foldedUrl.endsWith(` ${foldedName}`)) {
          electorateFolder = `${String(Number(numStr)).padStart(3, "0")}_${name}`;
          break;
        }
      }
    }
  }

  // If still no termKey, use "term_unknown"
  if (!termKey) termKey = "term_unknown";

  return { termKey, electorateFolder };
}

// ---------- BFS state ----------
function defaultState() {
  return {
    // levels[level] = { visited: [url], pages: [url], files: [{url, ext}] }
    levels: {}
  };
}

function loadState() {
  return readJsonIfExists(STATE_PATH, defaultState());
}

function saveState(st) {
  writeJson(STATE_PATH, st);
}

function levelPathUrls(level) {
  return path.join(LEVELS_DIR, `urls-level-${level}.json`);
}

function levelPathFiles(level) {
  return path.join(LEVELS_DIR, `files-level-${level}.json`);
}

function computeSeenFromState(st, upToLevelInclusive) {
  const seenPages = new Set();
  const seenFiles = new Set();

  const levels = Object.keys(st.levels)
    .map(k => Number(k))
    .filter(n => Number.isFinite(n))
    .sort((a, b) => a - b);

  for (const L of levels) {
    if (L > upToLevelInclusive) break;
    const rec = st.levels[String(L)];
    if (!rec) continue;

    for (const u of rec.visited || []) seenPages.add(normalizeUrl(u));
    for (const u of rec.pages || []) seenPages.add(normalizeUrl(u));

    for (const f of rec.files || []) {
      if (f?.url) seenFiles.add(normalizeUrl(f.url));
    }
  }

  return { seenPages, seenFiles };
}

// ---------- app ----------
const app = express();
app.use(express.json({ limit: "50mb" })); // large enough for base64 file payloads

ensureDir(CRAWL_ROOT);
ensureDir(LEVELS_DIR);
ensureDir(DOWNLOADS_DIR);

// Health
app.get("/", (_req, res) => {
  res.json({ ok: true, root: CRAWL_ROOT });
});

/**
 * POST /dedupe/level
 * body: {
 *   level: number,
 *   visited?: [{url}|string] or [string],
 *   pages?: [{url}|string] or [string],
 *   files?: [{url, ext}]  (optional; if omitted or empty => no files output)
 * }
 *
 * Semantics:
 *  - Replaces stored content for this level (rerun-safe)
 *  - Produces:
 *      urls-level-(level+1).json  ONLY if there are new unique pages remaining
 *      files-level-(level).json   ONLY if there are any files
 */
app.post("/dedupe/level", (req, res) => {
  try {
    const level = Number(req.body?.level);
    if (!Number.isFinite(level) || level < 1) {
      return res.status(400).json({ ok: false, error: "Invalid level" });
    }

    // Normalize inputs
    const inVisitedRaw = req.body?.visited || [];
    const inPagesRaw = req.body?.pages || [];
    const inFilesRaw = req.body?.files || [];

    const toUrlList = (raw) =>
      (Array.isArray(raw) ? raw : [])
        .filter(r => !isTruthyMetaRow(r))
        .map(r => (typeof r === "string" ? r : r?.url))
        .filter(Boolean)
        .map(normalizeUrl);

    const visited = stableUniq(toUrlList(inVisitedRaw));
    const pages = stableUniq(toUrlList(inPagesRaw));

    const files = stableUniq(
      (Array.isArray(inFilesRaw) ? inFilesRaw : [])
        .filter(r => r && !isTruthyMetaRow(r) && r.url)
        .map(r => ({
          url: normalizeUrl(r.url),
          ext: (r.ext || getExtFromUrl(r.url) || "bin").toLowerCase()
        }))
    );

    // Load state and REPLACE this level record
    const st = loadState();
    st.levels[String(level)] = { visited, pages, files };
    saveState(st);

    // Compute seen up to this level (includes visited + pages + files of <= level)
    const { seenPages, seenFiles } = computeSeenFromState(st, level);

    // Next level pages should be: current 'pages' filtered by all pages already seen in <= level
    // BUT: because pages are discovered *from* level input, they belong to (level+1).
    // So we filter against seen up to level (which includes visited seeds etc).
    const nextPages = pages.filter(u => !seenPages.has(u)); // NOTE: seenPages includes pages itself now
    // Wait—pages is already in seenPages because we stored it. We need to filter against levels < level+1
    // Correct approach: seen up to (level-1) PLUS visited of current level (to kill seed repeats),
    // but not including current pages themselves.
    const { seenPages: seenPagesBefore } = computeSeenFromState(st, level - 1);
    const seenForNext = new Set([...seenPagesBefore, ...visited.map(normalizeUrl)]);

    const nextPagesFiltered = pages.filter(u => !seenForNext.has(u));

    // Files at this level: similarly filter against files seen in previous levels (but allow overwrite on rerun)
    const { seenFiles: seenFilesBefore } = computeSeenFromState(st, level - 1);
    const filesFiltered = files.filter(f => !seenFilesBefore.has(f.url));

    // Write urls-level-(level+1).json ONLY if any next pages exist
    const nextLevel = level + 1;
    const nextUrlsPath = levelPathUrls(nextLevel);
    const urlsPayload = nextPagesFiltered.map(url => ({ url, level: nextLevel, kind: "urls" })); // meta conflated per-row style

    if (urlsPayload.length > 0) {
      writeJson(nextUrlsPath, urlsPayload);
    } else {
      // do not produce empty file; delete if exists
      unlinkIfExists(nextUrlsPath);
    }

    // Write files-level-(level).json ONLY if any files exist
    const filesPath = levelPathFiles(level);
    const filesPayload = filesFiltered.map(f => ({ url: f.url, ext: f.ext, level, kind: "files" }));

    if (filesPayload.length > 0) {
      writeJson(filesPath, filesPayload);
    } else {
      unlinkIfExists(filesPath);
    }

    return res.json({
      ok: true,
      level,
      wrote_next_urls: urlsPayload.length,
      wrote_files: filesPayload.length,
      next_urls_path: urlsPayload.length ? nextUrlsPath : null,
      files_path: filesPayload.length ? filesPath : null
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * POST /upload/file
 * body: { url, ext?, content_base64 }
 *
 * Saves into:
 *   BFS_crawl/downloads/<termKey>/<NNN_ElectorateName?>/<filename>
 *
 * Overwrites if exists (as you requested).
 */
app.post("/upload/file", (req, res) => {
  try {
    const url = normalizeUrl(req.body?.url);
    const ext = (req.body?.ext || getExtFromUrl(url) || "bin").toLowerCase();
    const b64 = req.body?.content_base64;

    if (!url || !b64) {
      return res.status(400).json({ ok: false, error: "Missing url or content_base64" });
    }

    const { termKey, electorateFolder } = inferPlacementFromUrl(url);

    const termDir = path.join(DOWNLOADS_DIR, termKey);
    const finalDir = electorateFolder ? path.join(termDir, electorateFolder) : termDir;

    ensureDir(finalDir);

    let filename = safeFileNameFromUrl(url);
    // If filename has no extension but ext provided, add it
    if (!/\.[a-z0-9]+$/i.test(filename) && ext) filename += `.${ext}`;

    const outPath = path.join(finalDir, filename);
    const buf = Buffer.from(b64, "base64");

    fs.writeFileSync(outPath, buf); // overwrite
    return res.json({ ok: true, saved_to: outPath, bytes: buf.length });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.listen(PORT, () => {
  console.log(`sink.js listening on http://localhost:${PORT}`);
  console.log(`CRAWL_ROOT: ${CRAWL_ROOT}`);
  console.log(`LEVELS_DIR: ${LEVELS_DIR}`);
  console.log(`DOWNLOADS_DIR: ${DOWNLOADS_DIR}`);
});
