const path = require("path");
const { normalizeUrl, extFromUrl } = require("./urlnorm");

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

function termKeyForUrl(u, electoratesByTerm) {
  const url = String(u || "");

  // GE archive URL: /electionresults_YYYY/
  let m = url.match(/\/electionresults_(\d{4})\//i);
  if (m) {
    const geYear = Number(m[1]);
    for (const k of Object.keys(electoratesByTerm || {})) {
      const p = termKeyParts(k);
      if (p && p.geYear === geYear) return k;
    }
  }

  // by-election / referendum includes event year
  m = url.match(/\/(\d{4})_[^/]*(byelection|by-election|referendum)\//i);
  let eventYear = m ? Number(m[1]) : null;
  if (!eventYear) {
    m = url.match(/(19\d{2}|20\d{2})/);
    if (m) eventYear = Number(m[1]);
  }
  if (!eventYear) return "term_unknown";

  const terms = Object.keys(electoratesByTerm || {})
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
  const t = (electoratesByTerm || {})[termKey];
  if (!t?.official_order) return null;

  const u = String(url || "");

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

  // Fallback: match electorate name tokens in URL
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

function safeFilename(name) {
  return (
    String(name || "download.bin")
      .replace(/[\/\\]/g, "_")
      .replace(/[\u0000-\u001f]/g, "")
      .slice(0, 240) || "download.bin"
  );
}

function filenameFromUrl(u) {
  try {
    const { URL } = require("url");
    const U = new URL(u);
    const base = path.basename(U.pathname);
    return safeFilename(base || "download.bin");
  } catch {
    return "download.bin";
  }
}

function resolveSavePath({ downloadsRoot, url, ext, source_page_url, electoratesByTerm }) {
  const fileUrl = normalizeUrl(url);
  const sourceUrl = source_page_url ? normalizeUrl(source_page_url) : null;
  const inferredExt = (ext || extFromUrl(fileUrl) || "bin").toLowerCase();

  let termKey = termKeyForUrl(fileUrl, electoratesByTerm);
  if (termKey === "term_unknown" && sourceUrl) {
    const tk2 = termKeyForUrl(sourceUrl, electoratesByTerm);
    if (tk2 && tk2 !== "term_unknown") termKey = tk2;
  }

  let electorateFolder = null;
  if (termKey !== "term_unknown") {
    electorateFolder = electorateFolderFor(termKey, fileUrl, electoratesByTerm);
    if (!electorateFolder && sourceUrl) {
      electorateFolder = electorateFolderFor(termKey, sourceUrl, electoratesByTerm);
    }
  }

  const termDir = path.join(downloadsRoot, termKey);
  const finalDir = electorateFolder ? path.join(termDir, electorateFolder) : termDir;

  let filename = filenameFromUrl(fileUrl);
  if (!/\.[a-z0-9]+$/i.test(filename) && inferredExt) filename += `.${inferredExt}`;
  filename = safeFilename(filename);

  return {
    termKey,
    electorateFolder,
    termDir,
    finalDir,
    filename,
    outPath: path.join(finalDir, filename),
    ext: inferredExt,
  };
}

module.exports = {
  termKeyForUrl,
  electorateFolderFor,
  resolveSavePath,
};
