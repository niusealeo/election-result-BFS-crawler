const fs = require("fs");
const path = require("path");

const { readJsonSafe } = require("./fsx");
const { stableUniqUrls } = require("./urlnorm");
const { writeFilesForLevel, writeChunkedFiles } = require("./artifacts");

function nowIso() {
  return new Date().toISOString();
}

function listDomainKeys(baseCfg) {
  const root = baseCfg.META_ROOT;
  if (!root || !fs.existsSync(root)) return [];
  return fs
    .readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name)
    .sort();
}

function listFileLevels(cfg) {
  const dir = cfg.ARTIFACT_DIR;
  if (!dir || !fs.existsSync(dir)) return [];
  const out = [];
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    if (!ent.isFile()) continue;
    const m = ent.name.match(/^files-level-(\d+)\.json$/i);
    if (!m) continue;
    const lvl = Number(m[1]);
    if (Number.isFinite(lvl)) out.push(lvl);
  }
  return out.sort((a, b) => a - b);
}

function readExpectedFileUrls(cfg, level) {
  const p = path.join(cfg.ARTIFACT_DIR, `files-level-${level}.json`);
  const arr = readJsonSafe(p, []);
  const urls = Array.isArray(arr)
    ? arr
        .map((r) => (typeof r === "string" ? r : r?.url))
        .filter(Boolean)
    : [];
  // Important: do NOT drop the first row; treat it like any other URL row.
  return stableUniqUrls(urls);
}

function readDownloadedFileUrlsForLevel(cfg, level) {
  const idx = readJsonSafe(cfg.DOWNLOADED_HASH_INDEX_PATH, {});
  const downloaded = new Set();
  for (const rec of Object.values(idx || {})) {
    const sources = Array.isArray(rec?.sources) ? rec.sources : [];
    for (const s of sources) {
      if (!s || !s.url) continue;
      const lvl = Number(s.level);
      if (Number.isFinite(lvl) && lvl === level) downloaded.add(String(s.url));
    }
  }
  return downloaded;
}

function reconcileFilesLevel({ cfg, level, chunkSize }) {
  const started_ts = nowIso();
  const expectedUrls = readExpectedFileUrls(cfg, level);
  const expectedSet = new Set(expectedUrls);
  const downloadedSet = readDownloadedFileUrlsForLevel(cfg, level);

  const remainingUrls = expectedUrls.filter((u) => !downloadedSet.has(u));

  const remainingRows = remainingUrls.map((u) => ({ url: u }));
  const remainingPath = path.join(cfg.ARTIFACT_DIR, `files-level-${level}.remaining.json`);
  writeFilesForLevel({ path: remainingPath, files: remainingRows, level, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });
  const chunkInfo = writeChunkedFiles({
    basePath: remainingPath,
    files: remainingRows,
    level,
    metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW,
    chunkSize,
  });

  const result = {
    domain_key: cfg.domain_key,
    level,
    chunk_size: chunkSize,
    expected: expectedSet.size,
    downloaded: downloadedSet.size,
    remaining: remainingRows.length,
    status: remainingRows.length ? "INCOMPLETE" : "COMPLETE",
    wrote: {
      remaining: remainingPath,
      parts: chunkInfo.chunk_files,
      parts_manifest: chunkInfo.manifest_path,
    },
    started_ts,
    finished_ts: nowIso(),
  };

  return result;
}

// Read-only status computation (NO writes).
function computeFilesLevelStatus({ cfg, level }) {
  const expectedUrls = readExpectedFileUrls(cfg, level);
  const expectedSet = new Set(expectedUrls);
  const downloadedSet = readDownloadedFileUrlsForLevel(cfg, level);
  const remaining = expectedUrls.filter((u) => !downloadedSet.has(u)).length;
  return {
    domain_key: cfg.domain_key,
    level,
    expected: expectedSet.size,
    downloaded: downloadedSet.size,
    remaining,
    status: remaining ? "INCOMPLETE" : "COMPLETE",
  };
}

module.exports = {
  listDomainKeys,
  listFileLevels,
  reconcileFilesLevel,
  computeFilesLevelStatus,
};
