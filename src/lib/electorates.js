const path = require("path");
const { readJsonSafe, writeJson, ensureDir } = require("./fsx");

function cleanElectorateName(name) {
  if (!name) return null;
  let s = String(name).trim();
  if (!s) return null;
  s = s.replace(/\s+/g, " ");
  if (s.toLowerCase() === "n/a") return null;
  return s;
}

function loadElectoratesMeta(path) {
  return readJsonSafe(path, {});
}

function saveElectoratesMeta(path, meta) {
  writeJson(path, meta);
}

/**
 * Create (if missing) the canonical electorate folders for a term.
 * Folder format: NNN_Electorate Name (NNN is 1-based official order).
 */
function ensureTermElectorateFolders({ downloadsRoot, termKey, electoratesByTerm }) {
  if (!downloadsRoot || !termKey) return;
  const t = (electoratesByTerm || {})[termKey];
  if (!t?.official_order) return;

  const termDir = path.join(downloadsRoot, termKey);
  ensureDir(termDir);

  const entries = Object.entries(t.official_order)
    .map(([k, v]) => ({ n: Number(k), name: v }))
    .filter((x) => Number.isFinite(x.n) && x.n > 0 && x.name)
    .sort((a, b) => a.n - b.n);

  for (const e of entries) {
    const folder = `${String(e.n).padStart(3, "0")}_${e.name}`;
    ensureDir(path.join(termDir, folder));
  }
}

module.exports = {
  cleanElectorateName,
  loadElectoratesMeta,
  saveElectoratesMeta,
  ensureTermElectorateFolders,
};
