const fs = require("fs");
const path = require("path");

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function unlinkIfExists(p) {
  try {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
}

function readJsonSafe(p, fallback) {
  try {
    if (!fs.existsSync(p)) return fallback;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

// Atomic JSON write: write temp file then rename into place.
// This prevents partial/truncated JSON if the process is interrupted.
function writeJson(p, obj) {
  ensureDir(path.dirname(p));
  const dir = path.dirname(p);
  const base = path.basename(p);
  const tmp = path.join(
    dir,
    `.${base}.tmp-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`
  );
  const data = JSON.stringify(obj, null, 2);
  fs.writeFileSync(tmp, data, "utf8");
  fs.renameSync(tmp, p);
}

module.exports = {
  ensureDir,
  unlinkIfExists,
  readJsonSafe,
  writeJson,
};
