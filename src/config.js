const path = require("path");

const PORT = Number(process.env.PORT || 3000);

// Root folders relative to process.cwd()
// NOTE: These are roots. Domain-scoped state is stored under these roots.
const BFS_ROOT = path.resolve(process.cwd(), "BFS_crawl");
const DOWNLOADS_ROOT = path.resolve(process.cwd(), "downloads");

// Domain-scoped roots
const META_ROOT = path.join(BFS_ROOT, "_meta");
const RUNS_ROOT = path.join(BFS_ROOT, "runs");

// Artifact output format: put conflated meta row first
// true => first row is {_meta:true, level, kind, ...real row...}, remaining rows minimal
// false => every row includes level/kind
const ARTIFACT_META_FIRST_ROW = process.env.ARTIFACT_META_FIRST_ROW !== "0";

// When writing very large level artifacts, also emit chunked variants so that
// Postman Runner can process them in smaller batches without crashing.
// Default chosen to match a conservative safe batch size.
const ARTIFACT_CHUNK_SIZE = Number(process.env.ARTIFACT_CHUNK_SIZE || 6169);

// ---------------------------------------------------------------------------
// Auto-finalize streaming runs
// ---------------------------------------------------------------------------
// When Postman crashes mid-run (common on 10k+ iterations), the streaming
// JSONL bucket is still safely written on disk, but the finalization step may
// never be called. These settings allow the sink to automatically finalize
// stale/idle runs.
//
// Enable with AUTO_FINALIZE_ENABLED=1 (default). Disable with 0.
const AUTO_FINALIZE_ENABLED = process.env.AUTO_FINALIZE_ENABLED !== "0";
// If no new data has been appended for this long, consider the run idle.
const AUTO_FINALIZE_IDLE_MS = Number(process.env.AUTO_FINALIZE_IDLE_MS || 180000); // 3 minutes
// How often to scan for idle runs.
const AUTO_FINALIZE_INTERVAL_MS = Number(process.env.AUTO_FINALIZE_INTERVAL_MS || 60000); // 1 minute

module.exports = {
  PORT,
  BFS_ROOT,
  DOWNLOADS_ROOT,
  META_ROOT,
  RUNS_ROOT,
  ARTIFACT_META_FIRST_ROW,
  ARTIFACT_CHUNK_SIZE,
  AUTO_FINALIZE_ENABLED,
  AUTO_FINALIZE_IDLE_MS,
  AUTO_FINALIZE_INTERVAL_MS,
};
