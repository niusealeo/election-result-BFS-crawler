const { normalizeUrl, extFromUrl } = require("./urlnorm");

/**
 * BFS-critical merge:
 * - Uniqueness key = file.url ONLY (normalized)
 * - Prefer non-null source_page_url
 * - Prefer non-"bin" ext
 */
function mergeFilesPreferSource(files) {
  const byUrl = new Map();

  for (const f of Array.isArray(files) ? files : []) {
    if (!f) continue;
    const url = f.url ? normalizeUrl(f.url) : "";
    if (!url) continue;

    const ext = (f.ext || extFromUrl(url) || "bin").toLowerCase();
    const source_page_url = f.source_page_url ? normalizeUrl(f.source_page_url) : null;

    const cur = byUrl.get(url);
    if (!cur) {
      byUrl.set(url, { url, ext, source_page_url });
      continue;
    }

    const merged = { ...cur };
    if ((!merged.ext || merged.ext === "bin") && ext && ext !== "bin") merged.ext = ext;
    if (!merged.source_page_url && source_page_url) merged.source_page_url = source_page_url;

    byUrl.set(url, merged);
  }

  return Array.from(byUrl.values());
}

module.exports = { mergeFilesPreferSource };
