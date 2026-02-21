const { readJsonSafe, writeJson } = require("./fsx");
const { normalizeUrl } = require("./urlnorm");

// Helpers for reconstructing state from artifacts (self-aware state cache)
const fs = require("fs");
const path = require("path");

function _extractUrlsFromArtifact(arr) {
  if (!Array.isArray(arr)) return [];
  const out = [];
  for (const r of arr) {
    const u = typeof r === "string" ? r : r?.url;
    if (u) out.push(normalizeUrl(u));
  }
  return out;
}

function _extractFilesFromArtifact(arr) {
  if (!Array.isArray(arr)) return [];
  const out = [];
  for (const r of arr) {
    if (!r) continue;
    const u = r.url;
    if (!u) continue;
    out.push({
      url: normalizeUrl(u),
      ext: r.ext || null,
      source_page_url: r.source_page_url || null,
    });
  }
  return out;
}

function _listMaxLevelFromArtifacts(artifactDir) {
  try {
    const names = fs.readdirSync(artifactDir);
    let max = 0;
    for (const n of names) {
      const m = String(n).match(/^urls-level-(\d+)\.json$/i);
      if (m) max = Math.max(max, Number(m[1]) || 0);
    }
    return max;
  } catch {
    return 0;
  }
}

/**
 * Rebuild (or reconcile) state.levels for a domain from artifacts on disk.
 *
 * Design goal: state.json is a cache. Canonical truth is the artifact files.
 * This prevents polluted running totals from earlier buggy versions.
 */
function reconcileStateFromArtifacts(cfg, st, opts = {}) {
  const artifactDir = cfg?.ARTIFACT_DIR;
  if (!artifactDir) return st;

  const maxLevel = Number.isFinite(Number(opts.maxLevel))
    ? Number(opts.maxLevel)
    : _listMaxLevelFromArtifacts(artifactDir);

  const next = { ...st, levels: { ...(st.levels || {}) } };

  // Reconstruct for levels 1..maxLevel based on:
  // - visited(L) = urls-level-L.json minus urls-level-L.remaining.json (if present)
  // - pages(L)   = urls-level-(L+1).json (discovered pages)
  // - files(L)   = files-level-L.json
  for (let L = 1; L <= maxLevel; L++) {
    const fullPath = path.join(artifactDir, `urls-level-${L}.json`);
    if (!fs.existsSync(fullPath)) continue;

    const fullArr = readJsonSafe(fullPath, []);
    const fullUrls = _extractUrlsFromArtifact(fullArr);

    const remPath = path.join(artifactDir, `urls-level-${L}.remaining.json`);
    let visitedUrls = [];
    if (fs.existsSync(remPath)) {
      const remArr = readJsonSafe(remPath, []);
      const remUrls = new Set(_extractUrlsFromArtifact(remArr));
      visitedUrls = fullUrls.filter((u) => !remUrls.has(u));
    } else {
      // No remaining file: don't guess. Preserve existing visited if any.
      visitedUrls = (next.levels[String(L)]?.visited || []).map(normalizeUrl);
    }

    const pagesPath = path.join(artifactDir, `urls-level-${L + 1}.json`);
    const pagesUrls = fs.existsSync(pagesPath) ? _extractUrlsFromArtifact(readJsonSafe(pagesPath, [])) : [];

    const filesPath = path.join(artifactDir, `files-level-${L}.json`);
    const files = fs.existsSync(filesPath) ? _extractFilesFromArtifact(readJsonSafe(filesPath, [])) : [];

    next.levels[String(L)] = {
      visited: [...new Set(visitedUrls)].sort(),
      pages: [...new Set(pagesUrls)].sort(),
      files,
    };
  }

  return next;
}

function defaultState() {
  return { levels: {}, file_hashes: {}, term_dirs_created: {} };
}

function loadState(statePath) {
  return readJsonSafe(statePath, defaultState());
}

function saveState(statePath, st) {
  writeJson(statePath, st);
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

module.exports = { loadState, saveState, computeSeenUpTo, reconcileStateFromArtifacts };
