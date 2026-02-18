const express = require("express");
const cfg = require("./config");
const { ensureDir } = require("./lib/fsx");

const { makeHealthRouter } = require("./routes/health");
const { makeDedupeRouter } = require("./routes/dedupe");
const { makeUploadRouter } = require("./routes/upload");
const { makeElectoratesRouter } = require("./routes/electorates");
const { makeRunsRouter } = require("./routes/runs");
const { makeProbeRouter } = require("./routes/probe");
const { resortDownloads } = require("./lib/resort");

function ensureFolders() {
  // Ensure folders exist (same as old sink.js)
  ensureDir(cfg.BFS_ROOT);
  ensureDir(cfg.RUNS_DIR);
  ensureDir(cfg.META_DIR);
  ensureDir(cfg.ARTIFACT_DIR);
  ensureDir(cfg.LEVEL_FILES_DIR);
  ensureDir(cfg.DOWNLOADS_ROOT);
}

function parseArgs(argv) {
  const args = { _: [] };
  for (const a of argv) {
    if (!a) continue;
    if (!a.startsWith("--")) {
      args._.push(a);
      continue;
    }
    const s = a.slice(2);
    const eq = s.indexOf("=");
    if (eq === -1) args[s] = true;
    else args[s.slice(0, eq)] = s.slice(eq + 1);
  }
  return args;
}

async function runServer() {
  ensureFolders();

  const app = express();
  app.use(express.json({ limit: "750mb" }));

  app.use(makeHealthRouter(cfg));
  app.use(makeDedupeRouter(cfg));
  app.use(makeUploadRouter(cfg));
  app.use(makeElectoratesRouter(cfg));
  app.use(makeRunsRouter(cfg));
  app.use(makeProbeRouter(cfg));

  app.listen(cfg.PORT, () => {
    console.log(`sink listening on http://localhost:${cfg.PORT}`);
    console.log(`BFS_ROOT: ${cfg.BFS_ROOT}`);
    console.log(`DOWNLOADS_ROOT: ${cfg.DOWNLOADS_ROOT}`);
    console.log(`META_DIR: ${cfg.META_DIR}`);
    console.log(`RUNS_DIR: ${cfg.RUNS_DIR}`);
    console.log(`ARTIFACT_META_FIRST_ROW: ${cfg.ARTIFACT_META_FIRST_ROW}`);
  });
}

async function runCli(cmd, args) {
  ensureFolders();

  if (cmd === "resort-downloads") {
    const dryRun = !args.apply; // default dry-run unless --apply
    const conflict = String(args.conflict || "suffix"); // suffix | skip | overwrite
    const root = String(args.root || cfg.DOWNLOADS_ROOT);
    const limit = args.limit ? Number(args.limit) : null;
    await resortDownloads({ cfg, downloadsRootOverride: root, dryRun, conflict, limit });
    return;
  }

  console.error(`Unknown command: ${cmd}`);
  console.error("Usage:");
  console.error("  node src/index.js resort-downloads [--apply] [--root=/path/to/downloads] [--conflict=suffix|skip|overwrite] [--limit=N]");
  process.exit(2);
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.length === 0) return runServer();

  const args = parseArgs(argv);
  const cmd = args._[0];
  if (!cmd) return runServer();
  return runCli(cmd, args);
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
