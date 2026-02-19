const path = require("path");

const { ensureDir } = require("./fsx");

function safeDomainKey(s) {
  const raw = String(s || "").trim().toLowerCase();
  const noWww = raw.replace(/^www\./, "");
  // Filesystem-safe: keep [a-z0-9.-], replace others with '_'
  const safe = noWww.replace(/[^a-z0-9.-]+/g, "_").replace(/^_+|_+$/g, "");
  return safe || "default";
}

function domainKeyFromUrl(u) {
  try {
    const { URL } = require("url");
    const U = new URL(String(u));
    return safeDomainKey(U.hostname);
  } catch {
    return null;
  }
}

function inferDomainKeyFromReq(req) {
  const b = req?.body || {};
  const q = req?.query || {};

  // Explicit override first
  const explicit = b.domain_key || b.domain || q.domain_key || q.domain;
  if (explicit) return safeDomainKey(explicit);

  // Preferred: crawl_root/root_url (stable per run)
  const crawlRoot = b.crawl_root || b.root_url || b.base_url || q.crawl_root || q.root_url || q.base_url;
  if (crawlRoot) {
    const k = domainKeyFromUrl(crawlRoot);
    if (k) return k;
  }

  // Fallback: direct url on this request
  const directUrl = b.url || q.url;
  if (directUrl) {
    const k = domainKeyFromUrl(directUrl);
    if (k) return k;
  }

  // Fallback: first url in arrays (dedupe step)
  const firstFromList = (arr) => {
    if (!Array.isArray(arr)) return null;
    for (const x of arr) {
      const u = typeof x === "string" ? x : x?.url;
      if (u) return u;
    }
    return null;
  };

  const u1 = firstFromList(b.visited) || firstFromList(b.pages) || firstFromList(b.files);
  if (u1) {
    const k = domainKeyFromUrl(u1);
    if (k) return k;
  }

  return "default";
}

function domainCfg(baseCfg, domainKey) {
  const dk = safeDomainKey(domainKey);
  const META_DIR = path.join(baseCfg.META_ROOT, dk);
  const RUNS_DIR = path.join(baseCfg.RUNS_ROOT, dk);
  const ARTIFACT_DIR = path.join(META_DIR, "artifacts");
  const LEVEL_FILES_DIR = path.join(META_DIR, "level_files");

  return {
    ...baseCfg,
    domain_key: dk,

    // Domain-scoped folders
    META_DIR,
    RUNS_DIR,
    ARTIFACT_DIR,
    LEVEL_FILES_DIR,
    DOWNLOADS_ROOT: path.join(baseCfg.DOWNLOADS_ROOT, dk),

    // Domain-scoped state files
    STATE_PATH: path.join(META_DIR, "state.json"),
    ELECTORATES_BY_TERM_PATH: path.join(META_DIR, "electorates_by_term.json"),
    DOWNLOADED_HASH_INDEX_PATH: path.join(META_DIR, "downloaded_hash_index.json"),
    PROBE_META_INDEX_PATH: path.join(META_DIR, "probe_meta_index.json"),

    // Domain-scoped logs
    LOG_DEDUPE: path.join(RUNS_DIR, "dedupe_log.jsonl"),
    LOG_FILE_SAVES: path.join(RUNS_DIR, "file_saves.jsonl"),
    // These are part of persistent crawl state and should live under _meta/<domain>/
    // so that a domain's full state can be reconstructed without depending on runs/.
    LOG_ELECTORATES_INGEST: path.join(META_DIR, "electorates_by_term.jsonl"),
    LOG_LEVEL_RESETS: path.join(META_DIR, "level_resets.jsonl"),
    LOG_META_PROBES: path.join(META_DIR, "meta_probes.jsonl"),
  };
}

function ensureDomainFolders(cfg) {
  ensureDir(cfg.META_DIR);
  ensureDir(cfg.RUNS_DIR);
  ensureDir(cfg.ARTIFACT_DIR);
  ensureDir(cfg.LEVEL_FILES_DIR);
  ensureDir(cfg.DOWNLOADS_ROOT);
}

function cfgForReq(baseCfg, req) {
  const dk = inferDomainKeyFromReq(req);
  const c = domainCfg(baseCfg, dk);
  ensureDomainFolders(c);
  return c;
}

module.exports = {
  safeDomainKey,
  domainKeyFromUrl,
  inferDomainKeyFromReq,
  domainCfg,
  ensureDomainFolders,
  cfgForReq,
};
