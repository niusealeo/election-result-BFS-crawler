const { readJsonSafe, writeJson } = require("./fsx");

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

module.exports = {
  cleanElectorateName,
  loadElectoratesMeta,
  saveElectoratesMeta,
};
