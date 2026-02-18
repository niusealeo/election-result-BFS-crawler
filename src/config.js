const path = require("path");

const PORT = Number(process.env.PORT || 3000);

// Root folders relative to process.cwd()
const BFS_ROOT = path.resolve(process.cwd(), "BFS_crawl");
const DOWNLOADS_ROOT = path.resolve(process.cwd(), "downloads"); // sibling of BFS_crawl

const RUNS_DIR = path.join(BFS_ROOT, "runs");
const META_DIR = path.join(BFS_ROOT, "_meta");
const ARTIFACT_DIR = path.join(META_DIR, "artifacts");

// Per-level manifest of downloaded files (stores relative paths)
const LEVEL_FILES_DIR = path.join(META_DIR, "level_files");

const STATE_PATH = path.join(META_DIR, "state.json");
const ELECTORATES_BY_TERM_PATH = path.join(META_DIR, "electorates_by_term.json");

// Content-hash index for downloaded files (stores relative paths)
const DOWNLOADED_HASH_INDEX_PATH = path.join(META_DIR, "downloaded_hash_index.json");

// Per-URL signature index (tracks latest observed content signature for a URL)
// Used for detecting same-URL content changes across update runs.
const URL_SIGNATURE_INDEX_PATH = path.join(META_DIR, "url_signature_index.json");

// Logs
const LOG_DEDUPE = path.join(RUNS_DIR, "dedupe_log.jsonl");
const LOG_FILE_SAVES = path.join(RUNS_DIR, "file_saves.jsonl");
const LOG_ELECTORATES_INGEST = path.join(RUNS_DIR, "electorates_ingest.jsonl");
const LOG_LEVEL_RESETS = path.join(RUNS_DIR, "level_resets.jsonl");
const LOG_FILE_MODS = path.join(RUNS_DIR, "file_mods.jsonl");

// Artifact output format: put conflated meta row first
// true => first row is {_meta:true, level, kind, ...real row...}, remaining rows minimal
// false => every row includes level/kind
const ARTIFACT_META_FIRST_ROW = process.env.ARTIFACT_META_FIRST_ROW !== "0";

module.exports = {
  PORT,
  BFS_ROOT,
  DOWNLOADS_ROOT,
  RUNS_DIR,
  META_DIR,
  ARTIFACT_DIR,
  LEVEL_FILES_DIR,
  STATE_PATH,
  ELECTORATES_BY_TERM_PATH,
  DOWNLOADED_HASH_INDEX_PATH,
  URL_SIGNATURE_INDEX_PATH,
  LOG_DEDUPE,
  LOG_FILE_SAVES,
  LOG_FILE_MODS,
  LOG_ELECTORATES_INGEST,
  LOG_LEVEL_RESETS,
  ARTIFACT_META_FIRST_ROW,
};
