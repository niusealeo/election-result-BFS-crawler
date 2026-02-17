const { readJsonSafe, writeJson } = require("./fsx");
const { normalizeUrl } = require("./urlnorm");

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

module.exports = { loadState, saveState, computeSeenUpTo };
