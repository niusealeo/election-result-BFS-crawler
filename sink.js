/**
 * sink.js — One big BFS crawl state, no IDs.
 *
 * Folders:
 *   ./BFS_crawl/
 *      runs/                 (logs)
 *      _meta/                (state + artifacts + electorates_by_term.json + hashes)
 *   ./downloads/             (SIBLING of BFS_crawl; actual downloaded files)
 *
 * Notes:
 * - Files are saved directly into your existing folder structure:
 *     downloads/<termKey>/[<electorateFolder>/]<filename>
 *   (NO extra /pdf/ directory)
 *
 * - Supports storing `source_page_url` alongside each file URL in artifacts and upload payload.
 * - Adds:
 *     (a) Fallback page fetch (optional) to infer term/electorate when unknown
 *     (b) SHA256 hashing to detect duplicates even if filenames differ
 *     (c) Strict PDF header validation + quarantine into downloads/<termKey>/_bad/...
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const { URL } = require("url");
const crypto = require("crypto");
const http = require("http");
const https = require("https");

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

// content hash index (for duplicate detection)
const HASH_INDEX_PATH = path.join(META_DIR, "hash_index.json");

// Optional network fallback toggle (default true)
const ENABLE_FALLBACK_FETCH = process.env.ENABLE_FALLBACK_FETCH !== "0";

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
  try {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
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
  return (
    String(name || "download.bin")
      .replace(/[\/\\]/g, "_")
      .replace(/[\u0000-\u001f]/g, "")
      .slice(0, 240) || "download.bin"
  );
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
    if (!seen.has(x)) {
      seen.add(x);
      out.push(x);
    }
  }
  return out;
}

function sha256Hex(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function sniffIsPdf(buf) {
  // PDF begins with "%PDF-"
  if (!buf || buf.length < 5) return false;
  return buf[0] === 0x25 && buf[1] === 0x50 && buf[2] === 0x44 && buf[3] === 0x46 && buf[4] === 0x2d;
}

function looksLikeHtml(buf) {
  const head = buf.slice(0, 512).toString("utf8").trim().toLowerCase();
  return head.startsWith("<!doctype html") || head.startsWith("<html") || head.includes("<head") || head.includes("<title");
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
const FETCH_LOG = path.join(RUNS_DIR, "fallback_fetch_log.jsonl");

// ----------------------- BFS state model -----------------------
/**
 * state.json:
 * {
 *   levels: {
 *     "1": { visited: [url...], pages: [url...], files: [{url, ext, source_page_url?}...] },
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
    .map((k) => Number(k))
    .filter((n) => Number.isFinite(n))
    .sort((a, b) => a - b);

  for (const L of levels) {
    if (L > maxLevelInclusive) break;
    const rec = st.levels[String(L)];
    if (!rec) continue;
    for (const u of rec.visited || []) seenPages.add(normalizeUrl(u));
    for (const u of rec.pages || []) seenPages.add(normalizeUrl(u));
    for (const f of rec.files || []) if (f?.url) seenFiles.add(normalizeUrl(f.url));
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
    .map((k) => ({ k, p: termKeyParts(k) }))
    .filter((x) => x.p)
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

// ----------------------- fallback fetch for context inference -----------------------
function httpGetText(u, { maxBytes = 2_000_000, timeoutMs = 25_000, maxRedirects = 5 } = {}) {
  return new Promise((resolve, reject) => {
    let urlStr;
    try {
      urlStr = new URL(u).toString();
    } catch {
      return reject(new Error("Invalid URL"));
    }

    const lib = urlStr.startsWith("https:") ? https : http;

    const req = lib.get(
      urlStr,
      {
        headers: {
          "User-Agent": "Mozilla/5.0 (sink.js)",
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
      },
      (res) => {
        const status = res.statusCode || 0;
        const loc = res.headers.location;

        // redirects
        if (status >= 300 && status < 400 && loc && maxRedirects > 0) {
          res.resume();
          const nextUrl = new URL(loc, urlStr).toString();
          return resolve(httpGetText(nextUrl, { maxBytes, timeoutMs, maxRedirects: maxRedirects - 1 }));
        }

        const chunks = [];
        let total = 0;

        res.on("data", (d) => {
          total += d.length;
          if (total > maxBytes) {
            res.destroy(new Error("Response too large"));
            return;
          }
          chunks.push(d);
        });

        res.on("end", () => {
          const buf = Buffer.concat(chunks);
          resolve({ status, finalUrl: urlStr, text: buf.toString("utf8") });
        });
      }
    );

    req.on("error", reject);
    req.setTimeout(timeoutMs, () => req.destroy(new Error("Timeout")));
  });
}

function extractCandidateContextUrlsFromHtml(html) {
  const out = new Set();

  // Look for electionresults_YYYY paths
  const reGE = /https?:\/\/[^"' ]+\/electionresults_(\d{4})\/[^"' ]*/gi;
  let m;
  while ((m = reGE.exec(html)) !== null) out.add(m[0]);

  // Look for /eNN/ electorate tokens
  const reE = /https?:\/\/[^"' ]+\/e(\d{1,3})\/[^"' ]*/gi;
  while ((m = reE.exec(html)) !== null) out.add(m[0]);

  // General absolute URLs
  // (Keep conservative; only if it looks like electionresults.govt.nz)
  const reHost = /https?:\/\/(?:www\.)?electionresults\.govt\.nz\/[^"' ]+/gi;
  while ((m = reHost.exec(html)) !== null) out.add(m[0]);

  return [...out].slice(0, 200);
}

async function inferFromSourcePage({ sourcePageUrl, electoratesByTerm }) {
  if (!ENABLE_FALLBACK_FETCH) return { inferredTermKey: null, inferredElectorateFolder: null, note: "fetch_disabled" };
  if (!sourcePageUrl) return { inferredTermKey: null, inferredElectorateFolder: null, note: "no_source_page" };

  const t0 = Date.now();
  try {
    const { status, finalUrl, text } = await httpGetText(sourcePageUrl);
    const html = String(text || "");
    const candidates = extractCandidateContextUrlsFromHtml(html);

    // Try term inference from:
    // - finalUrl
    // - any linked URLs on the page
    const urlsToTry = [finalUrl, ...candidates].map(normalizeUrl);

    let inferredTermKey = null;
    let inferredElectorateFolder = null;

    for (const u of urlsToTry) {
      const tk = termKeyForUrl(u, electoratesByTerm);
      if (tk && tk !== "term_unknown") {
        inferredTermKey = tk;
        break;
      }
    }

    if (inferredTermKey) {
      for (const u of urlsToTry) {
        const ef = electorateFolderFor(inferredTermKey, u, electoratesByTerm);
        if (ef) {
          inferredElectorateFolder = ef;
          break;
        }
      }
    }

    appendJsonl(FETCH_LOG, {
      ts: new Date().toISOString(),
      source_page_url: sourcePageUrl,
      status,
      ms: Date.now() - t0,
      inferredTermKey,
      inferredElectorateFolder,
      candidates_checked: urlsToTry.length,
    });

    return {
      inferredTermKey,
      inferredElectorateFolder,
      note: "fetched",
    };
  } catch (e) {
    appendJsonl(FETCH_LOG, {
      ts: new Date().toISOString(),
      source_page_url: sourcePageUrl,
      ms: Date.now() - t0,
      error: String(e?.message || e),
    });
    return { inferredTermKey: null, inferredElectorateFolder: null, note: "fetch_failed" };
  }
}

// ----------------------- hash index -----------------------
function loadHashIndex() {
  // { "<sha256>": { first_seen: "...", bytes: 123, locations: [{path,url,source_page_url,ts}] } }
  return readJsonSafe(HASH_INDEX_PATH, {});
}
function saveHashIndex(idx) {
  writeJson(HASH_INDEX_PATH, idx);
}
function upsertHashIndex(idx, hash, entry) {
  if (!idx[hash]) {
    idx[hash] = {
      first_seen: entry.ts,
      bytes: entry.bytes,
      locations: [entry],
    };
  } else {
    const locs = idx[hash].locations || [];
    locs.push(entry);
    idx[hash].locations = locs;
  }
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
    RUNS_DIR,
    ENABLE_FALLBACK_FETCH,
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
          .map((r) => (typeof r === "string" ? r : r?.url))
          .filter(Boolean)
          .map(normalizeUrl)
      );

    const visited = toUrlArray(req.body?.visited || []);
    const pages = toUrlArray(req.body?.pages || []);

    const inFiles = Array.isArray(req.body?.files) ? req.body.files : [];
    const files = stableUniq(
      inFiles
        .filter((f) => f?.url)
        .map((f) => ({
          url: normalizeUrl(f.url),
          ext: (f.ext || extFromUrl(f.url) || "bin").toLowerCase(),
          source_page_url: f.source_page_url ? normalizeUrl(f.source_page_url) : null,
        }))
        .map((f) => JSON.stringify(f))
    ).map((s) => JSON.parse(s));

    // Load + replace level record (rerun-safe)
    const st = loadState();
    st.levels[String(level)] = { visited, pages, files };
    saveState(st);

    // Build seen sets for filtering:
    // - Next level must exclude anything seen in levels < level
    // - Also exclude current level's visited seeds (so seed won't reappear)
    const { seenPages: seenPagesBefore, seenFiles: seenFilesBefore } = computeSeenUpTo(st, level - 1);
    const seenForNextPages = new Set([...seenPagesBefore, ...visited]);
    const nextPages = pages.filter((u) => !seenForNextPages.has(u));

    const filesOut = files.filter((f) => !seenFilesBefore.has(f.url));

    // Write artifacts into BFS_crawl/_meta/artifacts
    const nextLevel = level + 1;
    const nextUrlsPath = path.join(ARTIFACT_DIR, `urls-level-${nextLevel}.json`);
    const filesPath = path.join(ARTIFACT_DIR, `files-level-${level}.json`);

    if (nextPages.length > 0) {
      // per-row carries level/kind; no separate meta row
      writeJson(nextUrlsPath, nextPages.map((url) => ({ url, level: nextLevel, kind: "urls" })));
    } else {
      // IMPORTANT: do not produce an empty file
      unlinkIfExists(nextUrlsPath);
    }

    if (filesOut.length > 0) {
      writeJson(
        filesPath,
        filesOut.map((f) => ({
          url: f.url,
          ext: f.ext,
          level,
          kind: "files",
          source_page_url: f.source_page_url || null,
        }))
      );
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
      files_out: filesOut.length,
    });

    res.json({
      ok: true,
      level,
      next_level: nextLevel,
      wrote_next_urls: nextPages.length,
      wrote_files: filesOut.length,
      next_urls_path: nextPages.length ? nextUrlsPath : null,
      files_path: filesOut.length ? filesPath : null,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- 2) UPLOAD FILE (save into term/electorate structure) ----------
app.post("/upload/file", async (req, res) => {
  try {
    const url = normalizeUrl(req.body?.url);
    const ext = (req.body?.ext || extFromUrl(url) || "bin").toLowerCase();
    const b64 = req.body?.content_base64;

    // New: source page URL (optional)
    const sourcePageUrl = req.body?.source_page_url ? normalizeUrl(req.body.source_page_url) : null;

    if (!url || !b64) return res.status(400).json({ ok: false, error: "Missing url or content_base64" });

    const electoratesByTerm = loadElectoratesByTerm();

    // First-pass inference from file URL
    let termKey = termKeyForUrl(url, electoratesByTerm);
    let electorateFolder = termKey !== "term_unknown" ? electorateFolderFor(termKey, url, electoratesByTerm) : null;

    // If unknown or no electorate folder, try sourcePageUrl heuristics first (no fetch)
    if (sourcePageUrl) {
      if (termKey === "term_unknown") {
        const tk2 = termKeyForUrl(sourcePageUrl, electoratesByTerm);
        if (tk2 && tk2 !== "term_unknown") termKey = tk2;
      }
      if (!electorateFolder && termKey !== "term_unknown") {
        const ef2 = electorateFolderFor(termKey, sourcePageUrl, electoratesByTerm);
        if (ef2) electorateFolder = ef2;
      }
    }

    // If still not enough context, optionally fetch source page HTML and infer
    let fallbackNote = null;
    if ((termKey === "term_unknown" || !electorateFolder) && sourcePageUrl) {
      const inferred = await inferFromSourcePage({ sourcePageUrl, electoratesByTerm });
      fallbackNote = inferred.note;

      if (termKey === "term_unknown" && inferred.inferredTermKey) termKey = inferred.inferredTermKey;
      if (!electorateFolder && inferred.inferredElectorateFolder) electorateFolder = inferred.inferredElectorateFolder;
    }

    const termDir = path.join(DOWNLOADS_ROOT, termKey);
    const finalDir = electorateFolder ? path.join(termDir, electorateFolder) : termDir;
    ensureDir(finalDir);

    let filename = filenameFromUrl(url);
    if (!/\.[a-z0-9]+$/i.test(filename) && ext) filename += `.${ext}`;
    filename = safeFilename(filename);

    const buf = Buffer.from(String(b64), "base64");

    // Strict PDF validation + quarantine
    const shouldBePdf = ext === "pdf" || filename.toLowerCase().endsWith(".pdf");
    let note = "ok";
    let outDir = finalDir;
    let outPath = path.join(outDir, filename);

    if (shouldBePdf && !sniffIsPdf(buf)) {
      note = looksLikeHtml(buf) ? "bad_pdf_got_html" : "bad_pdf_not_pdf";
      outDir = path.join(termDir, "_bad");
      ensureDir(outDir);

      // Keep original basename but avoid overwriting by adding suffix
      const base = filename.replace(/\.pdf$/i, "");
      const badName = safeFilename(`${base}__${note}.html`);
      outPath = path.join(outDir, badName);
    }

    // Hashing (duplicate detection)
    const hash = sha256Hex(buf);
    const hashIdx = loadHashIndex();
    const isDuplicate = !!hashIdx[hash];

    // Write file (overwrite allowed; you can later change behavior if you want)
    fs.writeFileSync(outPath, buf);

    const ts = new Date().toISOString();

    // Update hash index
    upsertHashIndex(hashIdx, hash, {
      ts,
      bytes: buf.length,
      path: outPath,
      url,
      source_page_url: sourcePageUrl || null,
      termKey,
      electorateFolder: electorateFolder || null,
      note,
    });
    saveHashIndex(hashIdx);

    appendJsonl(FILE_SAVES_LOG, {
      ts,
      url,
      source_page_url: sourcePageUrl || null,
      termKey,
      electorateFolder: electorateFolder || null,
      saved_to: outPath,
      bytes: buf.length,
      ext,
      hash,
      duplicate_of: isDuplicate ? (hashIdx[hash]?.locations?.[0]?.path || null) : null,
      note,
      fallback_note: fallbackNote,
    });

    res.json({
      ok: true,
      saved_to: outPath,
      bytes: buf.length,
      termKey,
      electorateFolder: electorateFolder || null,
      note,
      hash,
      duplicate: isDuplicate,
      duplicate_of: isDuplicate ? (hashIdx[hash]?.locations?.[0]?.path || null) : null,
      fallback_note: fallbackNote,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------- 3) ELECTORATES INGEST (build electorates_by_term.json from Postman) ----------

// ---- helpers referenced by your endpoint ----
function loadElectoratesMeta() {
  return readJsonSafe(ELECTORATES_BY_TERM_PATH, {});
}

function saveElectoratesMeta(meta) {
  writeJson(ELECTORATES_BY_TERM_PATH, meta);
}

function cleanElectorateName(name) {
  if (!name) return null;
  let s = String(name).trim();
  if (!s) return null;

  // normalize whitespace
  s = s.replace(/\s+/g, " ");

  // avoid weird placeholders
  if (s.toLowerCase() === "n/a") return null;

  return s;
}

// ---- endpoints ----
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

  // rebuild alpha defensively from official_order (ignore client alphabetical_order)
  const names = Object.values(cleanedOfficial);
  const alpha = [...names].sort((a, b) => a.localeCompare(b, "en", { sensitivity: "base" }));
  const rebuiltAlpha = {};
  alpha.forEach((nm, i) => (rebuiltAlpha[nm] = i + 1));

  meta[termKey] = { official_order: cleanedOfficial, alphabetical_order: rebuiltAlpha };
  saveElectoratesMeta(meta);

  // log
  appendJsonl(ELECTORATES_INGEST_LOG, {
    ts: new Date().toISOString(),
    termKey,
    count: Object.keys(cleanedOfficial).length,
  });

  res.json({ ok: true, termKey, count: Object.keys(cleanedOfficial).length });
});

app.get("/meta/electorates", (_req, res) => {
  res.json(loadElectoratesMeta());
});

app.post("/meta/electorates/reset", (_req, res) => {
  saveElectoratesMeta({});
  appendJsonl(ELECTORATES_INGEST_LOG, { ts: new Date().toISOString(), action: "reset" });
  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`sink listening on http://localhost:${PORT}`);
  console.log(`BFS_ROOT: ${BFS_ROOT}`);
  console.log(`DOWNLOADS_ROOT (sibling): ${DOWNLOADS_ROOT}`);
  console.log(`META_DIR: ${META_DIR}`);
  console.log(`RUNS_DIR: ${RUNS_DIR}`);
  console.log(`electorates_by_term.json: ${ELECTORATES_BY_TERM_PATH}`);
  console.log(`hash_index.json: ${HASH_INDEX_PATH}`);
  console.log(`ENABLE_FALLBACK_FETCH: ${ENABLE_FALLBACK_FETCH}`);
});
