const { readJsonSafe, writeJson, ensureDir } = require("./fsx");
const path = require("path");
const { normalizeUrl } = require("./urlnorm");

function loadUrlSigIndex(p) {
  return readJsonSafe(p, {});
}

function saveUrlSigIndex(p, idx) {
  ensureDir(path.dirname(p));
  writeJson(p, idx);
}

function getUrlSig(idx, url) {
  const key = normalizeUrl(url);
  return idx[key] || null;
}

function setUrlSig(idx, url, rec) {
  const key = normalizeUrl(url);
  idx[key] = { ...(idx[key] || {}), ...(rec || {}), url: key };
}

module.exports = {
  loadUrlSigIndex,
  saveUrlSigIndex,
  getUrlSig,
  setUrlSig,
};
