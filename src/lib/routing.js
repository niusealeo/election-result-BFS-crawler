const path = require("path");
const { normalizeUrl, extFromUrl } = require("./urlnorm");
const { decodeHtmlEntities } = require("./html");

function asciiFold(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function compactFold(s) {
  // fold + remove non-alnum so "Auckland Central" matches "AucklandCentral"
  return asciiFold(decodeHtmlEntities(String(s || ""))).replace(/[^a-z0-9]+/g, "");
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
  // decode HTML entities in official names (term 49 uses &#257; etc)
  const official = Object.fromEntries(
    Object.entries(t.official_order).map(([k, v]) => [k, decodeHtmlEntities(v)])
  );

  // Decode filename from URL for pattern matching
  let filenameRaw = "";
  try {
    const { URL } = require("url");
    const U = new URL(u);
    filenameRaw = path.basename(U.pathname) || "";
    try {
      filenameRaw = decodeURIComponent(filenameRaw);
    } catch {
      // keep raw
    }
  } catch {
    filenameRaw = (u.split("/").pop() || "");
  }

  const filenameFold = asciiFold(filenameRaw);
  const urlFold = asciiFold(u);
  const filenameCompact = compactFold(filenameRaw);

  // IMPORTANT: In GE archive URLs (electionresults_YYYY), the path segment /e9/ is the *election id*,
  // not an electorate. Do NOT treat it as electorate 9.
  const isArchiveElectionIdPath = /\/electionresults_\d{4}\/e\d{1,3}\//i.test(u);

  // Special case for terms 47–51 (2002–2014) exports:
  // Files are prefixed with "e9_" (election id), but many include electorate numbers at the end:
  //   e9_part8_cand_63.csv  -> electorate 63
  //   e9_part8_party_1.csv  -> electorate 1
  // Treat *_cand_###.* and *_part_###.* / *_party_###.* as electorate-numbered.
  let mE9 = filenameRaw.match(/^e9_.*?_(?:cand|candidate)_(\d{1,3})\.[a-z0-9]+$/i);
  if (mE9) {
    const n = Number(mE9[1]);
    const name = official[String(n)];
    if (name) return `${String(n).padStart(3, "0")}_${name}`;
  }
  mE9 = filenameRaw.match(/^e9_.*?_(?:part|party)_(\d{1,3})\.[a-z0-9]+$/i);
  if (mE9) {
    const n = Number(mE9[1]);
    const name = official[String(n)];
    if (name) return `${String(n).padStart(3, "0")}_${name}`;
  }

  // PRIORITY 1: URL path contains /eNN/ (but ignore GE archive election-id paths like /electionresults_1999/e9/)
  let m = u.match(/\/e(\d{1,3})\//i);
  if (m && !isArchiveElectionIdPath) {
    const n = Number(m[1]);
    const name = official[String(n)];
    if (name) return `${String(n).padStart(3, "0")}_${name}`;
  }

  // PRIORITY 2a: explicit electorate/voting-place numbering in filename
  m = filenameRaw.match(/(?:^|[_\-\s])(electorate|voting-place)[_\-\s]?(\d{1,3})(?=\D|$)/i);
  if (m) {
    const n = Number(m[2]);
    const name = official[String(n)];
    if (name) return `${String(n).padStart(3, "0")}_${name}`;
  }

  // PRIORITY 2b: suffix -NN / _NN where stem is known to be electorate-numbered
  // Examples in your corpus:
  //   candidate-votes-by-voting-place-1.csv
  //   party-votes-by-voting-place-52.csv
  //   split-votes-electorate-12.csv
  //   elect-splitvote-99.csv
  const m2 = filenameRaw.match(/^(.+?)[_\-](\d{1,3})(\.[a-z0-9]+)?$/i);
  if (m2) {
    const stemFold = asciiFold(m2[1])
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "");
    const n = Number(m2[2]);

    const electorateNumberedStems = new Set([
      "candidate-votes-by-voting-place",
      "party-votes-by-voting-place",
      "split-votes-electorate",
      "elect-splitvote",
      "electorate",
      "voting-place",
    ]);

    if (electorateNumberedStems.has(stemFold)) {
      const name = official[String(n)];
      if (name) return `${String(n).padStart(3, "0")}_${name}`;
    }
  }

  // Pattern /YYYY_slug_byelection/
  m = u.match(/\/\d{4}_([^/]+?)_(?:byelection|by-election)\//i);
  if (m) {
    const guess = asciiFold(m[1].replace(/[_-]+/g, " "));
    for (const [numStr, name] of Object.entries(official)) {
      if (asciiFold(name) === guess) {
        return `${String(Number(numStr)).padStart(3, "0")}_${name}`;
      }
    }
  }

  // PRIORITY 3: electorate name appears in filename (or URL as fallback)
  for (const [numStr, name] of Object.entries(official)) {
    const foldedName = asciiFold(name);
    if (!foldedName) continue;

    // normal containment
    const matchLoose = filenameFold.includes(foldedName) || urlFold.includes(foldedName);

    // compact containment: "AucklandCentral" vs "Auckland Central"
    const nameCompact = compactFold(name);
    const matchCompact = nameCompact && filenameCompact.includes(nameCompact);

    if (matchLoose || matchCompact) {
      const n = Number(numStr);
      return `${String(n).padStart(3, "0")}_${name}`;
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
    const baseRaw = path.basename(U.pathname);
    let base = baseRaw;
    try {
      base = decodeURIComponent(baseRaw);
    } catch {
      base = baseRaw;
    }
    return safeFilename(base || "download.bin");
  } catch {
    return "download.bin";
  }
}

function resolveSavePath({ downloadsRoot, url, ext, source_page_url, electoratesByTerm, filenameOverride }) {
  const fileUrl = normalizeUrl(url);
  const sourceUrl = source_page_url ? normalizeUrl(source_page_url) : null;
  const inferredExt = (ext || extFromUrl(fileUrl) || "bin").toLowerCase();

  // By-elections are treated as "state changes" within a term.
  // Store ALL by-election files under a term-level folder instead of an electorate folder.
  const isByElection = /(byelection|by-election)/i.test(fileUrl) || (sourceUrl ? /(byelection|by-election)/i.test(sourceUrl) : false);

  // --- By-election term inference ---
  // Prefer an explicit event year from the by-election URL/filename over the source page year,
  // because the by-elections index page may be hosted under a newer GE year.
  function extractYearFromText(t) {
    const m = String(t || "").match(/\b(19\d{2}|20\d{2})\b/);
    return m ? Number(m[1]) : null;
  }

  const geDates = {
    1996: "1996-10-12",
    1999: "1999-11-27",
    2002: "2002-07-27",
    2005: "2005-09-17",
    2008: "2008-11-08",
    2011: "2011-11-26",
    2014: "2014-09-20",
    2017: "2017-09-23",
    2020: "2020-10-17",
    2023: "2023-10-14",
  };

  function parseByElectionDateFromName(name) {
    const s = decodeHtmlEntities(String(name || ""));
    const m = s.match(/\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(19\d{2}|20\d{2})\b/i);
    if (!m) return null;
    const day = Number(m[1]);
    const monName = String(m[2]).toLowerCase();
    const year = Number(m[3]);
    const months = {
      january: 1, february: 2, march: 3, april: 4, may: 5, june: 6,
      july: 7, august: 8, september: 9, october: 10, november: 11, december: 12,
    };
    const mon = months[monName];
    if (!mon || !day || !year) return null;
    const mm = String(mon).padStart(2, "0");
    const dd = String(day).padStart(2, "0");
    return { year, iso: `${year}-${mm}-${dd}` };
  }

  function termKeyForEventYear(eventYear) {
    const terms = Object.keys(electoratesByTerm || {})
      .map((k) => ({ k, p: termKeyParts(k) }))
      .filter((x) => x.p)
      .sort((a, b) => a.p.geYear - b.p.geYear);

    if (!terms.length || !eventYear) return "term_unknown";

    // Outside known range -> create a separate folder bucket
    const minYear = terms[0].p.geYear;
    const maxYear = terms[terms.length - 1].p.geYear;
    if (eventYear < minYear || eventYear > maxYear) return `term_extra_(${eventYear})`;

    for (let i = 0; i < terms.length; i++) {
      const cur = terms[i];
      const next = terms[i + 1];
      if (!next) return cur.k;
      if (eventYear >= cur.p.geYear && eventYear < next.p.geYear) return cur.k;
    }
    return terms[terms.length - 1]?.k || "term_unknown";
  }

  function termKeyForEventDate(isoDate) {
    if (!isoDate) return "term_unknown";
    const y = Number(String(isoDate).slice(0, 4));
    const geIso = geDates[y];
    if (!geIso) return termKeyForEventYear(y);

    // If the by-election happened BEFORE the general election in the same year,
    // it belongs to the previous term bucket.
    if (isoDate < geIso) {
      return termKeyForEventYear(y - 1);
    }
    return termKeyForEventYear(y);
  }

  let termKey = termKeyForUrl(fileUrl, electoratesByTerm);

  if (isByElection) {
    const nameForDate = filenameOverride || filenameFromUrl(fileUrl);
    const d = parseByElectionDateFromName(nameForDate);

    const yFile = extractYearFromText(fileUrl) || extractYearFromText(nameForDate);

    if (d?.iso) {
      termKey = termKeyForEventDate(d.iso);
    } else if (yFile) {
      termKey = termKeyForEventYear(yFile);
    } else {
      termKey = "term_unknown";
    }
  }

  if (termKey === "term_unknown" && sourceUrl) {
    const tk2 = termKeyForUrl(sourceUrl, electoratesByTerm);
    if (tk2 && tk2 !== "term_unknown") termKey = tk2;
  }

  let electorateFolder = null;
  if (termKey !== "term_unknown" && !isByElection) {
    electorateFolder = electorateFolderFor(termKey, fileUrl, electoratesByTerm);
    if (!electorateFolder && sourceUrl) {
      electorateFolder = electorateFolderFor(termKey, sourceUrl, electoratesByTerm);
    }
  }

  const termDir = (isByElection && termKey === "term_unknown") ? downloadsRoot : path.join(downloadsRoot, termKey);
  const finalDir = isByElection
    ? ((termKey === "term_unknown") ? path.join(downloadsRoot, "by-elections") : path.join(termDir, "by-elections"))
    : (electorateFolder ? path.join(termDir, electorateFolder) : termDir);

  let filename = filenameOverride ? safeFilename(filenameOverride) : filenameFromUrl(fileUrl);
  if (!/\.[a-z0-9]+$/i.test(filename) && inferredExt) filename += `.${inferredExt}`;
  filename = safeFilename(filename);

  return {
    termKey,
    electorateFolder,
    isByElection,
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
