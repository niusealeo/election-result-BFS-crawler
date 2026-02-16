const path = require("path");

const PORT = Number(process.env.PORT || 3000);

// Root folders relative to process.cwd()
const BFS_ROOT = path.resolve(process.cwd(), "BFS_crawl");
const DOWNLOADS_ROOT = path.resolve(process.cwd(), "downloads"); // sibling of BFS_crawl

const RUNS_DIR = path.join(BFS_ROOT, "runs");
const META_DIR = path.join(BFS_ROOT, "_meta");
const ARTIFACT_DIR = path.join(META_DIR, "artifacts");

const STATE_PATH = path.join(META_DIR, "state.json");
const ELECTORATES_BY_TERM_PATH = path.join(META_DIR, "electorates_by_term.json");

// Logs
const LOG_DEDUPE = path.join(RUNS_DIR, "dedupe_log.jsonl");
const LOG_FILE_SAVES = path.join(RUNS_DIR, "file_saves.jsonl");
const LOG_ELECTORATES_INGEST = path.join(RUNS_DIR, "electorates_ingest.jsonl");

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
  STATE_PATH,
  ELECTORATES_BY_TERM_PATH,
  LOG_DEDUPE,
  LOG_FILE_SAVES,
  LOG_ELECTORATES_INGEST,
  ARTIFACT_META_FIRST_ROW,
};
