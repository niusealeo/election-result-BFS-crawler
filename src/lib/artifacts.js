const { writeJson, unlinkIfExists } = require("./fsx");

function chunkArray(arr, chunkSize) {
  const out = [];
  const n = Math.max(1, Number(chunkSize) || 1);
  for (let i = 0; i < (arr || []).length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function makeChunkFileName(basePath, chunkIndex, chunkCount) {
  // urls-level-6.json -> urls-level-6.part-0001-of-0003.json
  const p = String(basePath);
  const m = p.match(/^(.*)\.json$/i);
  const stem = m ? m[1] : p;
  const pad = (v, w) => String(v).padStart(w, "0");
  const w = Math.max(4, String(chunkCount || 0).length);
  return `${stem}.part-${pad(chunkIndex, w)}-of-${pad(chunkCount, w)}.json`;
}

/**
 * Write urls-level-(nextLevel).json and files-level-(level).json
 * Supports two output formats:
 * - metaFirstRow = true: first row is {_meta:true, level, kind, ...real row...}, rest minimal
 * - metaFirstRow = false: every row includes level/kind (legacy)
 */
function writeUrlArtifact({ path, urls, nextLevel, metaFirstRow }) {
  if (!urls || urls.length === 0) return unlinkIfExists(path);

  if (!metaFirstRow) {
    return writeJson(path, urls.map((url) => ({ url, level: nextLevel, kind: "urls" })));
  }

  const first = urls[0];
  const rest = urls.slice(1);
  return writeJson(path, [{ _meta: true, level: nextLevel, kind: "urls", url: first }, ...rest.map((url) => ({ url }))]);
}

// Write a urls artifact for an explicit level (not necessarily nextLevel).
function writeUrlsForLevel({ path, urls, level, metaFirstRow }) {
  if (!urls || urls.length === 0) return unlinkIfExists(path);

  if (!metaFirstRow) {
    return writeJson(path, urls.map((url) => ({ url, level, kind: "urls" })));
  }

  const first = urls[0];
  const rest = urls.slice(1);
  return writeJson(path, [{ _meta: true, level, kind: "urls", url: first }, ...rest.map((url) => ({ url }))]);
}

// Write chunked variants of a urls artifact, and a small manifest.
function writeChunkedUrls({ basePath, urls, level, metaFirstRow, chunkSize }) {
  if (!urls || urls.length === 0) {
    // remove any old chunk manifest if present
    return { chunk_files: [], manifest_path: null };
  }

  const chunks = chunkArray(urls, chunkSize);
  const chunk_files = [];
  for (let i = 0; i < chunks.length; i++) {
    const p = makeChunkFileName(basePath, i + 1, chunks.length);
    writeUrlsForLevel({ path: p, urls: chunks[i], level, metaFirstRow });
    chunk_files.push(p);
  }

  const manifest_path = `${String(basePath).replace(/\.json$/i, "")}.parts.json`;
  writeJson(manifest_path, {
    kind: "urls",
    level,
    chunk_size: Math.max(1, Number(chunkSize) || 1),
    total: urls.length,
    parts: chunk_files.map((p, idx) => ({
      index: idx + 1,
      path: p,
      count: chunks[idx].length,
    })),
  });

  return { chunk_files, manifest_path };
}

function writeFileArtifact({ path, files, level, metaFirstRow }) {
  if (!files || files.length === 0) return unlinkIfExists(path);

  if (!metaFirstRow) {
    return writeJson(
      path,
      files.map((f) => ({
        url: f.url,
        ext: f.ext,
        source_page_url: f.source_page_url || null,
        level,
        kind: "files",
      }))
    );
  }

  const first = files[0];
  const rest = files.slice(1);
  return writeJson(path, [{ _meta: true, level, kind: "files", ...first }, ...rest]);
}

/**
 * Write a flat list of rows for Postman Runner (diffs, probes, etc).
 * When metaFirstRow is true, the first array element is a conflated meta+row object.
 */
function writeRowListArtifact({ path, rows, kind, level, metaFirstRow, extraMeta }) {
  if (!rows || rows.length === 0) return unlinkIfExists(path);

  const ts = new Date().toISOString();

  if (!metaFirstRow) {
    return writeJson(path, rows.map((r) => ({ ...r, level, kind, ts, ...(extraMeta || {}) })));
  }

  const first = rows[0];
  const rest = rows.slice(1);
  return writeJson(path, [{ _meta: true, level, kind, ts, ...(extraMeta || {}), ...first }, ...rest]);
}

module.exports = {
  writeUrlArtifact,
  writeFileArtifact,
  writeRowListArtifact,
  writeUrlsForLevel,
  writeChunkedUrls,
};
