const express = require("express");
const fs = require("fs");
const path = require("path");

const { ensureDir, readJsonSafe, writeJson } = require("../lib/fsx");
const { appendJsonl } = require("../lib/jsonl");
const { toAbsolute } = require("../lib/paths");
const { withLock } = require("../lib/lock");
const { cfgForReq, domainCfg, ensureDomainFolders } = require("../lib/domain");
const { stableUniqUrls, extFromUrl } = require("../lib/urlnorm");
const { mergeFilesPreferSource } = require("../lib/dedupe");
const { loadState, saveState, computeSeenUpTo } = require("../lib/state");
const { writeUrlArtifact, writeFileArtifact, writeUrlsForLevel, writeChunkedUrls } = require("../lib/artifacts");
const { logEvent } = require("../lib/logger");
const { listDomainKeys, listFileLevels, reconcileFilesLevel } = require("../lib/reconcile_files");

const readline = require("readline");

// ---------------------------------------------------------------------------
// Large-run URL discovery support
// ---------------------------------------------------------------------------
// Postman can hit memory limits when it tries to accumulate tens of thousands
// of URLs in collection variables. These endpoints allow Postman to stream
// per-page discoveries to the sink and finalize once per run.
//
// Workflow:
//  1) POST /runs/start/urls    { level, run_id }
//  2) POST /runs/append/urls   { level, run_id, visited:[{url}], pages:[{url}], files:[{url,ext,source_page_url}] }
//  3) POST /runs/finalize/urls { level, run_id }
//
// The sink stores run events as JSONL and dedupes at finalize time.
// A coarse global lock serializes appends/finalization to avoid race conditions.

function safeRunId(v) {
  const raw = String(v || "").trim();
  // allow: letters, digits, dash, underscore, dot
  const safe = raw.replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 120);
  return safe || `run_${Date.now()}`;
}

function runJsonlPath(cfg, level, runId) {
  ensureDir(cfg.RUNS_DIR);
  return path.join(cfg.RUNS_DIR, `discover_level_${String(level)}_${safeRunId(runId)}.jsonl`);
}

function runDoneMarkerPath(jsonlPath) {
  return `${jsonlPath}.done`;
}

function markRunDone(jsonlPath, payload) {
  try {
    fs.writeFileSync(runDoneMarkerPath(jsonlPath), JSON.stringify({ ts: new Date().toISOString(), ...(payload || {}) }, null, 2), { encoding: "utf-8" });
  } catch {}
}

// When Postman streams a run, the first call (/runs/start/urls) may not include any URL
// (and thus no domain can be inferred). Subsequent /append calls *do* include URLs and
// will be correctly domain-scoped. However, the /finalize call might again include only
// {level, run_id}. In that case, cfgForReq() would fall back to "default".
//
// To keep streaming mode robust *and* backwards compatible, we locate the run JSONL by
// scanning domain run folders for the matching filename and picking the largest file.
function locateRunJsonlAcrossDomains(baseCfg, level, runId) {
  const safeId = safeRunId(runId);
  const fileName = `discover_level_${String(level)}_${safeId}.jsonl`;
  const root = baseCfg.RUNS_ROOT;

  if (!root || !fs.existsSync(root)) return null;

  let best = null; // { domainKey, path, size }
  const entries = fs.readdirSync(root, { withFileTypes: true });
  for (const ent of entries) {
    if (!ent.isDirectory()) continue;
    const domainKey = ent.name;
    const p = path.join(root, domainKey, fileName);
    if (!fs.existsSync(p)) continue;
    let size = 0;
    try { size = fs.statSync(p).size || 0; } catch {}
    if (!best || size > best.size) best = { domainKey, path: p, size };
  }
  return best;
}

async function readDiscoveryJsonl(p) {
  // Returns { visited:Set, pages:Set, files: Map(url->fileObj) }
  const visited = new Set();
  const pages = new Set();
  const files = new Map();

  if (!fs.existsSync(p)) return { visited, pages, files };

  const rl = readline.createInterface({
    input: fs.createReadStream(p, { encoding: "utf-8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const s = String(line || "").trim();
    if (!s) continue;
    let obj;
    try { obj = JSON.parse(s); } catch { continue; }
    if (!obj || typeof obj !== "object") continue;

    const addUrls = (arr, set) => {
      if (!Array.isArray(arr)) return;
      for (const x of arr) {
        const u = typeof x === "string" ? x : x?.url;
        if (u) set.add(u);
      }
    };

    addUrls(obj.visited, visited);
    addUrls(obj.pages, pages);

    if (Array.isArray(obj.files)) {
      for (const f of obj.files) {
        if (!f || !f.url) continue;
        const prev = files.get(f.url);
        const merged = prev ? { ...prev, ...f } : { ...f };
        files.set(f.url, merged);
      }
    }
  }

  return { visited, pages, files };
}

// Finalize helper used by both HTTP endpoint and the auto-finalize watchdog.
// IMPORTANT: Caller should hold withLock() if needed.
async function finalizeDiscoveryRun({ baseCfg, cfg, level, run_id, jsonlPath }) {
  ensureDomainFolders(cfg);

  const p = jsonlPath || runJsonlPath(cfg, level, run_id);
  const acc = await readDiscoveryJsonl(p);
  const visited = stableUniqUrls(Array.from(acc.visited));
  const pages = stableUniqUrls(Array.from(acc.pages));

  const inFiles = Array.from(acc.files.values());
  const filesMerged = mergeFilesPreferSource(inFiles);

  const st = loadState(cfg.STATE_PATH);
  st.levels[String(level)] = { visited, pages, files: filesMerged };
  saveState(cfg.STATE_PATH, st);

  const { seenPages: seenPagesBefore, seenFiles: seenFilesBefore } = computeSeenUpTo(st, level - 1);
  const seenForNextPages = new Set([...seenPagesBefore, ...visited]);

  const nextPages = pages.filter((u) => !seenForNextPages.has(u));
  const filesOut = filesMerged.filter((f) => !seenFilesBefore.has(f.url));

  const filesForArtifact = filesOut.map((f) => ({
    url: f.url,
    ext: (f.ext || extFromUrl(f.url) || "bin").toLowerCase(),
    source_page_url: f.source_page_url || null,
  }));

  const nextLevel = level + 1;
  const nextUrlsPath = path.join(cfg.ARTIFACT_DIR, `urls-level-${nextLevel}.json`);
  const filesPath = path.join(cfg.ARTIFACT_DIR, `files-level-${level}.json`);

  logEvent("FINALIZE_BEGIN", {
    mode: "streaming",
    domain_key: cfg.domain_key,
    level,
    run_id,
    jsonl: p,
  });

  writeUrlArtifact({ path: nextUrlsPath, urls: nextPages, nextLevel, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });
  writeFileArtifact({ path: filesPath, files: filesForArtifact, level, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });

  // -------------------------------------------------------------------
  // Resumable + chunked artifacts
  // -------------------------------------------------------------------
  // 1) Chunk the *next level* urls output so the user can feed Postman in
  //    smaller runs without crashing.
  const chunkSize = Number(baseCfg.ARTIFACT_CHUNK_SIZE || 6169);
  const nextChunkInfo = writeChunkedUrls({
    basePath: nextUrlsPath,
    urls: nextPages,
    level: nextLevel,
    metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW,
    chunkSize,
  });

  // 2) Produce a remaining list for the *current level* so a crash can be
  //    resumed by crawling only what was not visited.
  //    Prefer the input artifact for this level if present.
  let inputUrls = [];
  try {
    const inPath = path.join(cfg.ARTIFACT_DIR, `urls-level-${level}.json`);
    const arr = readJsonSafe(inPath, []);
    if (Array.isArray(arr)) {
      inputUrls = arr.map((r) => (typeof r === "string" ? r : r?.url)).filter(Boolean);
    }
  } catch {}

  // If input artifact isn't available (rare), we can still create remaining
  // by treating the discovered 'visited' set as the whole input.
  const visitedSet = new Set(visited);
  const remaining = (inputUrls && inputUrls.length)
    ? stableUniqUrls(inputUrls).filter((u) => !visitedSet.has(u))
    : [];

  const remainingPath = path.join(cfg.ARTIFACT_DIR, `urls-level-${level}.remaining.json`);
  writeUrlsForLevel({ path: remainingPath, urls: remaining, level, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });
  const remainingChunkInfo = writeChunkedUrls({
    basePath: remainingPath,
    urls: remaining,
    level,
    metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW,
    chunkSize,
  });

  appendJsonl(cfg.LOG_DEDUPE, {
    ts: new Date().toISOString(),
    level,
    run_id,
    mode: "streaming",
    visited: visited.length,
    pages: pages.length,
    next_pages: nextPages.length,
    files_in: inFiles.length,
    files_out: filesForArtifact.length,
  });

  markRunDone(p, { level, run_id, domain_key: cfg.domain_key, wrote: { next_urls: nextUrlsPath, files: filesPath } });

  logEvent("FINALIZE_DONE", {
    mode: "streaming",
    domain_key: cfg.domain_key,
    level,
    run_id,
    visited: visited.length,
    pages: pages.length,
    next_pages: nextPages.length,
    files_out: filesForArtifact.length,
    remaining: remaining.length,
    wrote_next_urls: nextUrlsPath,
    wrote_files: filesPath,
  });

  return {
    ok: true,
    level,
    run_id,
    visited: visited.length,
    pages: pages.length,
    next_pages: nextPages.length,
    files: filesForArtifact.length,
    remaining: remaining.length,
    wrote: {
      next_urls: nextUrlsPath,
      next_urls_parts: nextChunkInfo.chunk_files,
      next_urls_parts_manifest: nextChunkInfo.manifest_path,
      files: filesPath,
      remaining_urls: remainingPath,
      remaining_urls_parts: remainingChunkInfo.chunk_files,
      remaining_urls_parts_manifest: remainingChunkInfo.manifest_path,
    },
  };
}

function manifestPath(cfg, level) {
  return path.join(cfg.LEVEL_FILES_DIR, `${String(level)}.json`);
}

function levelsFromSources(rec) {
  const out = new Set();
  for (const s of rec?.sources || []) {
    const n = Number(s?.level);
    if (Number.isFinite(n)) out.add(n);
  }
  return [...out].sort((a,b)=>a-b);
}

function minLevelFromSources(rec) {
  const levels = levelsFromSources(rec);
  if (!levels.length) return Infinity;
  return Math.min(...levels);
}

function makeRunsRouter(baseCfg) {
  const r = express.Router();

  // ---------------------------------------------------------------------
  // URL discovery runs (streaming, memory-safe for very large levels)
  // ---------------------------------------------------------------------

  // Start (or reset) a discovery run bucket for a given level.
  // Body: { level: number, run_id?: string }
  r.post("/runs/start/urls", async (req, res) => {
    try {
      const cfg = cfgForReq(baseCfg, req);
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }
      const run_id = safeRunId(req.body?.run_id);

      return await withLock(() => {
        const p = runJsonlPath(cfg, level, run_id);
        ensureDir(path.dirname(p));
        // Hard reset: overwrite file
        fs.writeFileSync(p, "", { encoding: "utf-8" });
        // Clear any prior done marker for this run bucket.
        try { if (fs.existsSync(runDoneMarkerPath(p))) fs.unlinkSync(runDoneMarkerPath(p)); } catch {}
        appendJsonl(cfg.LOG_LEVEL_RESETS, {
          ts: new Date().toISOString(),
          kind: "urls",
          level,
          run_id,
          action: "start",
        });
        logEvent("RUN_START_URLS", {
          domain_key: cfg.domain_key,
          level,
          run_id,
          jsonl: p,
        });
        return res.json({ ok: true, level, run_id, path: p });
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  // Append a batch of discoveries.
  // Body: { level, run_id, visited?:[{url}], pages?:[{url}], files?:[...] }
  r.post("/runs/append/urls", async (req, res) => {
    try {
      const cfg = cfgForReq(baseCfg, req);
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }
      const run_id = safeRunId(req.body?.run_id);

      const visited = Array.isArray(req.body?.visited) ? req.body.visited : [];
      const pages = Array.isArray(req.body?.pages) ? req.body.pages : [];
      const files = Array.isArray(req.body?.files) ? req.body.files : [];

      // Light validation + trimming to keep JSONL clean
      const toRows = (arr) =>
        (Array.isArray(arr) ? arr : [])
          .map((r) => (typeof r === "string" ? { url: r } : r))
          .filter((r) => r && r.url);

      const payload = {
        ts: new Date().toISOString(),
        level,
        run_id,
        visited: toRows(visited),
        pages: toRows(pages),
        files: files.filter((f) => f && f.url),
      };

      return await withLock(() => {
        const p = runJsonlPath(cfg, level, run_id);
        appendJsonl(p, payload);
        logEvent("RUN_APPEND_URLS", {
          domain_key: cfg.domain_key,
          level,
          run_id,
          appended_visited: payload.visited.length,
          appended_pages: payload.pages.length,
          appended_files: payload.files.length,
          jsonl: p,
        });
        return res.json({ ok: true, level, run_id, appended: {
          visited: payload.visited.length,
          pages: payload.pages.length,
          files: payload.files.length,
        }});
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  // Finalize a discovery run: dedupe, update state, and write artifacts.
  // Body: { level, run_id }
  r.post("/runs/finalize/urls", async (req, res) => {
    try {
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }
      const run_id = safeRunId(req.body?.run_id);

      // Resolve domain config. If the request doesn't contain any URL/domain hints,
      // cfgForReq() will fall back to 'default'. In streaming mode, appends are
      // domain-scoped (because they include URLs), so we must locate the actual
      // JSONL bucket across domains.
      let cfg = cfgForReq(baseCfg, req);
      const hasExplicitDomainHint = Boolean(
        req.body?.domain_key || req.body?.domain || req.query?.domain_key || req.query?.domain ||
        req.body?.crawl_root || req.body?.root_url || req.body?.base_url || req.query?.crawl_root || req.query?.root_url || req.query?.base_url ||
        req.body?.url || req.query?.url
      );

      if (!hasExplicitDomainHint && cfg.domain_key === "default") {
        const found = locateRunJsonlAcrossDomains(baseCfg, level, run_id);
        if (found && found.domainKey) {
          cfg = domainCfg(baseCfg, found.domainKey);
          ensureDomainFolders(cfg);
        }
      }

      return await withLock(async () => {
        // Prefer the resolved domain-scoped file; if missing, fall back to any located file.
        let p = runJsonlPath(cfg, level, run_id);
        if (!fs.existsSync(p)) {
          const found = locateRunJsonlAcrossDomains(baseCfg, level, run_id);
          if (found?.path && fs.existsSync(found.path)) p = found.path;
        }

        const result = await finalizeDiscoveryRun({ baseCfg, cfg, level, run_id, jsonlPath: p });
        logEvent("RUN_FINALIZE_URLS", {
          domain_key: cfg.domain_key,
          level,
          run_id,
          visited: result?.visited,
          next_pages: result?.next_pages,
          files: result?.files,
          remaining: result?.remaining,
        });
        return res.json(result);
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  // Utility: chunk an existing urls-level-N.json (or any compatible list) into
  // smaller parts so Postman can run it without crashing.
  // Body: { level, chunk_size? }
  // Writes: urls-level-N.json (unchanged), plus urls-level-N.json.part-....json and manifest.
  r.post("/runs/chunk/urls", async (req, res) => {
    try {
      const cfg = cfgForReq(baseCfg, req);
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }
      const chunkSize = Number(req.body?.chunk_size || baseCfg.ARTIFACT_CHUNK_SIZE || 6169);
      const inPath = path.join(cfg.ARTIFACT_DIR, `urls-level-${level}.json`);
      const arr = readJsonSafe(inPath, []);
      const urls = Array.isArray(arr) ? arr.map((r) => (typeof r === "string" ? r : r?.url)).filter(Boolean) : [];
      const info = writeChunkedUrls({ basePath: inPath, urls: stableUniqUrls(urls), level, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW, chunkSize });
      logEvent("CHUNK_URLS", {
        domain_key: cfg.domain_key,
        level,
        chunk_size: chunkSize,
        total: urls.length,
        parts: info?.chunk_files?.length,
        manifest: info?.manifest_path,
      });
      return res.json({ ok: true, level, chunk_size: chunkSize, total: urls.length, wrote: info });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  // Hard reset for a BFS file-download level, BUT keep any file also used
  // by an earlier level (< level).
  //
  // Body: { level: number }
  r.post("/runs/start/files", async (req, res) => {
    try {
      const cfg = cfgForReq(baseCfg, req);
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }
      return await withLock(() => {
        ensureDir(cfg.LEVEL_FILES_DIR);

      const mPath = manifestPath(cfg, level);
      const manifest = readJsonSafe(mPath, { level, files: [] });

      const idx = readJsonSafe(cfg.DOWNLOADED_HASH_INDEX_PATH, {});

      let deletedFiles = 0;
      let keptBecauseEarlier = 0;
      let missingFiles = 0;
      let removedLevelRefs = 0;
      let deletedHashes = 0;

      for (const item of manifest.files || []) {
        const sha = item?.sha256;
        const savedRel = item?.saved_to;

        const rec = sha ? idx[sha] : null;

        // Remove this level's membership by dropping matching source observations.
        if (rec?.sources && Array.isArray(rec.sources)) {
          const before = rec.sources.length;
          rec.sources = rec.sources.filter((s) => Number(s?.level) !== level);
          if (rec.sources.length !== before) removedLevelRefs += (before - rec.sources.length);
        }

        // Keep if any earlier level still references it.
        const earlierMin = rec ? minLevelFromSources(rec) : Infinity;
        const usedByEarlier = earlierMin < level;
        if (usedByEarlier) {
          keptBecauseEarlier++;
          continue;
        }

        if (savedRel) {
          const savedAbs = toAbsolute(savedRel);
          if (fs.existsSync(savedAbs)) {
            try {
              fs.unlinkSync(savedAbs);
              deletedFiles++;
            } catch {
              // best-effort
            }
          } else {
            missingFiles++;
          }
        }

        // If no remaining levels, drop the hash record.
        // If no remaining sources, drop the hash record.
        if (rec && (!rec.sources || rec.sources.length === 0)) {
          delete idx[sha];
          deletedHashes++;
        } else if (rec) {
          rec.last_seen_ts = new Date().toISOString();
        }
      }

      // Overwrite manifest for this level as fresh.
      writeJson(mPath, { level, files: [], started_ts: new Date().toISOString() });
      writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);

      appendJsonl(cfg.LOG_LEVEL_RESETS, {
        ts: new Date().toISOString(),
        kind: "files",
        level,
        deletedFiles,
        keptBecauseEarlier,
        missingFiles,
        removedLevelRefs,
        deletedHashes,
      });

      return res.json({
        ok: true,
        level,
        deletedFiles,
        keptBecauseEarlier,
        missingFiles,
        removedLevelRefs,
        deletedHashes,
      });
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  // ---------------------------------------------------------------------
  // Files reconciliation + chunking (Option A)
  // ---------------------------------------------------------------------
  // POST /runs/chunk/files
  // Body: { level: number, domain?: string, chunk_size?: number }
  // If domain is provided, only that domain is reconciled.
  // If domain is omitted, all domains are swept for that level.
  r.post("/runs/chunk/files", async (req, res) => {
    try {
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }
      const chunkSize = Number(req.body?.chunk_size || baseCfg.ARTIFACT_CHUNK_SIZE || 6169);

      const domain = req.body?.domain ? String(req.body.domain) : null;
      const domains = domain ? [domain] : listDomainKeys(baseCfg);
      const results = [];

      return await withLock(() => {
        for (const dk of domains) {
          const cfg = domainCfg(baseCfg, dk);
          ensureDomainFolders(cfg);

          // Only reconcile if the expected artifact exists.
          const expectedPath = path.join(cfg.ARTIFACT_DIR, `files-level-${level}.json`);
          if (!fs.existsSync(expectedPath)) {
            results.push({ domain_key: cfg.domain_key, level, ok: false, status: "MISSING_EXPECTED", expected_path: expectedPath });
            continue;
          }

          const r1 = reconcileFilesLevel({ cfg, level, chunkSize });
          // Print reconciliation result to console (with timestamps)
          console.log(`[${new Date().toISOString()}] [RECONCILE files] domain=${cfg.domain_key} level=${level}`);
          console.log(`  expected:   ${r1.expected}`);
          console.log(`  downloaded: ${r1.downloaded}`);
          console.log(`  remaining:  ${r1.remaining}`);
          console.log(`  chunk_size: ${r1.chunk_size}`);
          console.log(`  parts:      ${Array.isArray(r1.wrote?.parts) ? r1.wrote.parts.length : 0}`);
          console.log(`  status:     ${r1.status}`);
          results.push({ ok: true, ...r1 });
        }
        return res.json({ ok: true, level, chunk_size: chunkSize, results });
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  // POST /runs/chunk/files/incomplete
  // Body: { domain?: string, chunk_size?: number }
  // Sweeps all domains (or one domain) and reconciles all *incomplete* file levels.
  r.post("/runs/chunk/files/incomplete", async (req, res) => {
    try {
      const chunkSize = Number(req.body?.chunk_size || baseCfg.ARTIFACT_CHUNK_SIZE || 6169);
      const domain = req.body?.domain ? String(req.body.domain) : null;
      const domains = domain ? [domain] : listDomainKeys(baseCfg);
      const results = [];

      return await withLock(() => {
        for (const dk of domains) {
          const cfg = domainCfg(baseCfg, dk);
          ensureDomainFolders(cfg);
          const levels = listFileLevels(cfg);
          for (const level of levels) {
            const r1 = reconcileFilesLevel({ cfg, level, chunkSize });
            // Only keep results where incomplete.
            if (r1.remaining > 0) {
              console.log(`[${new Date().toISOString()}] [RECONCILE files/incomplete] domain=${cfg.domain_key} level=${level}`);
              console.log(`  expected:   ${r1.expected}`);
              console.log(`  downloaded: ${r1.downloaded}`);
              console.log(`  remaining:  ${r1.remaining}`);
              console.log(`  chunk_size: ${r1.chunk_size}`);
              console.log(`  parts:      ${Array.isArray(r1.wrote?.parts) ? r1.wrote.parts.length : 0}`);
              console.log(`  status:     ${r1.status}`);
              results.push({ ok: true, ...r1 });
            }
          }
        }
        return res.json({ ok: true, chunk_size: chunkSize, results, incomplete_count: results.length });
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeRunsRouter, finalizeDiscoveryRun };
