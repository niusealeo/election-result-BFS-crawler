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

/**
 * Infer the termKey for an event year using:
 * - scraped electorates_by_term.json terms (best)
 * - then infer past/future terms on a 3-year cadence (NZ pattern) if needed
 * - return null if cannot infer safely (caller can fallback to term_extra_(YYYY))
 */
function inferTermKeyFromEventYear(eventYear, electoratesByTerm) {
  if (!Number.isFinite(eventYear)) return null;

  const terms = Object.keys(electoratesByTerm || {})
    .map((k) => ({ k, p: termKeyParts(k) }))
    .filter((x) => x.p && Number.isFinite(x.p.termNo) && Number.isFinite(x.p.geYear))
    .sort((a, b) => a.p.geYear - b.p.geYear);

  if (!terms.length) return null;

  const min = terms[0].p;
  const max = terms[terms.length - 1].p;

  // If event is within scraped range (or after earliest), pick greatest GE year <= eventYear.
  let candidate = null;
  for (const t of terms) {
    if (t.p.geYear <= eventYear) candidate = t;
    else break;
  }
  if (candidate) return candidate.k;

  // Event predates earliest scraped term. Try infer a prior GE year on 3-year cadence.
  // Keep term_extra_(YYYY) fallback if cadence doesn't fit.
  const diff = min.geYear - eventYear;

  // number of 3-year steps back so inferredGeYear <= eventYear
  const stepsBack = Math.ceil(diff / 3);
  const inferredGeYear = min.geYear - stepsBack * 3;

  if (inferredGeYear <= eventYear && (min.geYear - inferredGeYear) % 3 === 0) {
    const termNo = min.termNo - ((min.geYear - inferredGeYear) / 3);
    if (Number.isFinite(termNo) && termNo > 0) {
      return `term_${termNo}_(${inferredGeYear})`;
    }
  }

  // Event after max scraped term (future) — try infer forward on 3-year cadence.
  const diffF = eventYear - max.geYear;
  if (diffF > 0) {
    const stepsF = Math.floor(diffF / 3);
    const inferredGeYearF = max.geYear + stepsF * 3;
    if (inferredGeYearF <= eventYear && (inferredGeYearF - max.geYear) % 3 === 0) {
      const termNo = max.termNo + ((inferredGeYearF - max.geYear) / 3);
      if (Number.isFinite(termNo) && termNo > 0) {
        return `term_${termNo}_(${inferredGeYearF})`;
      }
    }
  }

  return null;
}

function termKeyForUrl(u, electoratesByTerm) {
  const url = String(u || "");

  // GE archive URL: /electionresults_YYYY/
  let m = url.match(/\/electionresults_(\d{4})\//i);
  if (m) {
    const geYear = Number(m[1]);
    const inferred = inferTermKeyFromEventYear(geYear, electoratesByTerm);
    if (inferred) return inferred;
    return `term_extra_(${geYear})`;
  }

  // by-election / referendum includes event year in path
  m = url.match(/\/(\d{4})_[^/]*(byelection|by-election|referenda?|referendum)\//i);
  let eventYear = m ? Number(m[1]) : null;

  // fallback: first 4-digit year anywhere
  if (!eventYear) {
    m = url.match(/\b(19\d{2}|20\d{2})\b/);
    if (m) eventYear = Number(m[1]);
  }

  if (!eventYear) return "term_unknown";

  const inferred2 = inferTermKeyFromEventYear(eventYear, electoratesByTerm);
  if (inferred2) return inferred2;

  // Dedicated bucket for inconsistencies (unknown past/future).
  return `term_extra_(${eventYear})`;
}

function termKeyForEvent(eventYear, monthOpt, electoratesByTerm) {
  // monthOpt: 1-12, used for by-elections/referenda when we can parse a date.
  // If an event occurs early in a GE year (e.g. Feb 2017), it belongs to the prior term.
  if (!Number.isFinite(eventYear)) return "term_unknown";

  const base = inferTermKeyFromEventYear(eventYear, electoratesByTerm) || `term_extra_(${eventYear})`;
  // If we can't parse a month, we still attempt a best-effort correction for
  // state-change events that occur in a GE year (e.g. 2017 by-elections that are
  // almost always *pre*-GE). This avoids misrouting into the *new* term when the
  // only signal available is the year in the URL path.
  const hasMonth = monthOpt && Number.isFinite(monthOpt);

  const terms = Object.keys(electoratesByTerm || {})
    .map((k) => ({ k, p: termKeyParts(k) }))
    .filter((x) => x.p)
    .sort((a, b) => a.p.geYear - b.p.geYear);

  // Only apply early-year override when this year is a known GE year in the scraped list.
  const idx = terms.findIndex((t) => t.p.geYear === eventYear);
  if (idx > 0) {
    // If month known: only flip for Jan–Jun.
    if (hasMonth && monthOpt <= 6) return terms[idx - 1].k;

    // If month unknown: prefer prior term (conservative for by-elections/referenda).
    // Rationale: these pages are typically organized by the Parliament/term the event
    // occurred *during*, and GE-year events overwhelmingly happen before the GE.
    // if (!hasMonth) return terms[idx - 1].k;
  }

  return base;
}

function parseMonthFromNameOrUrl(s) {
  const t = decodeHtmlEntities(String(s || ""));
  const m = t.match(
    /\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b/i
  );
  if (!m) return null;
  const monthName = m[2].toLowerCase();
  const map = {
    january: 1,
    february: 2,
    march: 3,
    april: 4,
    may: 5,
    june: 6,
    july: 7,
    august: 8,
    september: 9,
    october: 10,
    november: 11,
    december: 12,
  };
  return map[monthName] || null;
}

function electorateFolderFor(termKey, url, electoratesByTerm) {
  const t = (electoratesByTerm || {})[termKey];
  if (!t?.official_order) return null;

  const u = String(url || "");

  // Decode HTML entities in official names (term 49 uses &#257; etc)
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
    filenameRaw = u.split("/").pop() || "";
  }

  const filenameFold = asciiFold(filenameRaw);
  const urlFold = asciiFold(u);
  const filenameCompact = compactFold(filenameRaw);

  // IMPORTANT: In GE archive URLs (electionresults_YYYY), the path segment /e9/ is the *election id*,
  // not an electorate. Do NOT treat it as electorate 9.
  const isArchiveElectionIdPath = /\/electionresults_\d{4}\/e\d{1,3}\//i.test(u);

  // PRIORITY 1: URL path contains /eNN/ (but ignore GE archive election-id paths like /electionresults_1999/e9/)
  let m = u.match(/\/e(\d{1,3})\//i);
  if (m && !isArchiveElectionIdPath) {
    const n = Number(m[1]);
    const name = official[String(n)];
    if (name) return `${String(n).padStart(3, "0")}_${name}`;
  }

  // SPECIAL: Terms 47–51 exports often look like e9_part8_cand_63.csv
  // Here e9 = election id, and only trailing cand_## or party_## indicates electorate.
  const looksLikeElectionIdPrefix = /^e\d{1,3}_/i.test(filenameRaw);

  if (isArchiveElectionIdPath || looksLikeElectionIdPrefix) {

    // cand_##
    let mm = filenameRaw.match(/(?:^|[_-])cand[_-]?(\d{1,3})(?=\D|$)/i);
    if (mm) {
      const n = Number(mm[1]);
      const name = official[String(n)];
      if (name) return `${String(n).padStart(3, "0")}_${name}`;
    }

    // party_##
    mm = filenameRaw.match(/(?:^|[_-])party[_-]?(\d{1,3})(?=\D|$)/i);
    if (mm) {
      const n = Number(mm[1]);
      const name = official[String(n)];
      if (name) return `${String(n).padStart(3, "0")}_${name}`;
    }
  }


  // PRIORITY 2a: explicit electorate/voting-place numbering in filename
  m = filenameRaw.match(/(?:^|[_\-\s])(electorate|voting-place)[_\-\s]?(\d{1,3})(?=\D|$)/i);
  if (m) {
    const n = Number(m[2]);
    const name = official[String(n)];
    if (name) return `${String(n).padStart(3, "0")}_${name}`;
  }

  // PRIORITY 2b: suffix -NN / _NN where stem is known to be electorate-numbered
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

    const matchLoose = filenameFold.includes(foldedName) || urlFold.includes(foldedName);
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

  // State-change bundles
  const isByElection = /(byelection|by-election)/i.test(fileUrl) || (sourceUrl ? /(byelection|by-election)/i.test(sourceUrl) : false);
  const isReferendum = /(referenda?|referendum)/i.test(fileUrl) || (sourceUrl ? /(referenda?|referendum)/i.test(sourceUrl) : false);
  const isStateChange = isByElection || isReferendum;

  // Parse month for early-year override (e.g. Feb 2017 should be prior term)
  const fnameForDate = filenameOverride ? String(filenameOverride) : filenameFromUrl(fileUrl);
  const monthOpt =
    parseMonthFromNameOrUrl(fnameForDate) ||
    (sourceUrl ? parseMonthFromNameOrUrl(sourceUrl) : null) ||
    parseMonthFromNameOrUrl(fileUrl);

  // Match a 4-digit year that is not part of a longer number.
  // Works for "2016_flag_..." (underscore is fine).
  const yearMatch = fileUrl.match(/(?:^|[^0-9])(19\d{2}|20\d{2})(?=[^0-9]|$)/);
  const eventYear = yearMatch ? Number(yearMatch[1]) : NaN;

  // Targeted exception: 2017 Mt Albert by-election was pre-GE 2017 (no month in URLs).
  // Keep it in term 51 even though year is a GE year.
  const forcedPreGEYears = new Set([
    2017,
    2011
  ]);
  // (You can add similar targeted exceptions here if needed.)
  const forcedPreGESlugs = [
    "mt_albert_byelection",
    "te_tai_tokerau_byelection",
    "botany_byelection"
  ];

  const isBeforeGE =
    isByElection &&
    forcedPreGEYears.has(eventYear) &&
    forcedPreGESlugs.some(slug =>
      new RegExp(`/${eventYear}_${slug}/`, "i").test(fileUrl)
    );

  let termKey = isStateChange
    ? (isBeforeGE
      ? termKeyForEvent(eventYear, 6, electoratesByTerm)
      : termKeyForEvent(eventYear, monthOpt, electoratesByTerm))
    : termKeyForUrl(fileUrl, electoratesByTerm);


  if (termKey === "term_unknown" && sourceUrl) {
    const tk2 = termKeyForUrl(sourceUrl, electoratesByTerm);
    if (tk2 && tk2 !== "term_unknown") termKey = tk2;
  }

  let electorateFolder = null;
  if (termKey !== "term_unknown" && !isStateChange) {
    electorateFolder = electorateFolderFor(termKey, fileUrl, electoratesByTerm);
    if (!electorateFolder && sourceUrl) {
      electorateFolder = electorateFolderFor(termKey, sourceUrl, electoratesByTerm);
    }
  }

  // If we can't infer a term, keep it at downloads root (not inside term_unknown).
  const termDir = termKey === "term_unknown" ? downloadsRoot : path.join(downloadsRoot, termKey);

  const finalDir = isByElection
    ? path.join(termDir, "by-elections")
    : isReferendum
      ? path.join(termDir, "referenda")
      : (electorateFolder ? path.join(termDir, electorateFolder) : termDir);

  let filename = filenameOverride ? safeFilename(filenameOverride) : filenameFromUrl(fileUrl);
  if (!/\.[a-z0-9]+$/i.test(filename) && inferredExt) filename += `.${inferredExt}`;
  filename = safeFilename(filename);

  return {
    termKey,
    electorateFolder,
    isByElection,
    isReferendum,
    termDir,
    finalDir,
    filename,
    outPath: path.join(finalDir, filename),
    ext: inferredExt,
  };
}

module.exports = {
  termKeyForUrl,
  termKeyForEvent,
  electorateFolderFor,
  resolveSavePath,
};
