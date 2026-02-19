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

module.exports = {
  PORT,
  BFS_ROOT,
  DOWNLOADS_ROOT,
  META_ROOT,
  RUNS_ROOT,
  ARTIFACT_META_FIRST_ROW,
};
