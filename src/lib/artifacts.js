const { writeJson, unlinkIfExists } = require("./fsx");

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

module.exports = { writeUrlArtifact, writeFileArtifact, writeRowListArtifact };
