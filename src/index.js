const express = require("express");
const baseCfg = require("./config");
const { ensureDir } = require("./lib/fsx");
const { ensureDomainFolders, domainCfg, safeDomainKey } = require("./lib/domain");

const { makeHealthRouter } = require("./routes/health");
const { makeDedupeRouter } = require("./routes/dedupe");
const { makeUploadRouter } = require("./routes/upload");
const { makeElectoratesRouter } = require("./routes/electorates");
const { makeRunsRouter, finalizeDiscoveryRun } = require("./routes/runs");
const { makeProbeRouter } = require("./routes/probe");
const { resortDownloads } = require("./lib/resort");
const { startAutoFinalize } = require("./lib/autofinalize");

function ensureFolders() {
  // Ensure root folders exist (domain-scoped subfolders are created on demand)
  ensureDir(baseCfg.BFS_ROOT);
  ensureDir(baseCfg.META_ROOT);
  ensureDir(baseCfg.RUNS_ROOT);
  ensureDir(baseCfg.DOWNLOADS_ROOT);
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

  app.use(makeHealthRouter(baseCfg));
  app.use(makeDedupeRouter(baseCfg));
  app.use(makeUploadRouter(baseCfg));
  app.use(makeElectoratesRouter(baseCfg));
  app.use(makeRunsRouter(baseCfg));
  app.use(makeProbeRouter(baseCfg));

  // Auto-finalize stale streaming runs (helps when Postman crashes on 10k+ iterations).
  startAutoFinalize({ baseCfg, finalizeDiscoveryRun }).catch((e) => {
    console.log(`[auto-finalize] failed to start: ${String(e?.message || e)}`);
  });

  app.listen(baseCfg.PORT, () => {
    console.log(`sink listening on http://localhost:${baseCfg.PORT}`);
    console.log(`BFS_ROOT: ${baseCfg.BFS_ROOT}`);
    console.log(`DOWNLOADS_ROOT (root): ${baseCfg.DOWNLOADS_ROOT}`);
    console.log(`META_ROOT: ${baseCfg.META_ROOT}`);
    console.log(`RUNS_ROOT: ${baseCfg.RUNS_ROOT}`);
    console.log(`ARTIFACT_META_FIRST_ROW: ${baseCfg.ARTIFACT_META_FIRST_ROW}`);
    console.log(`Domain state lives under: BFS_crawl/_meta/<domain>/ and BFS_crawl/runs/<domain>/`);
    console.log(`Domain downloads live under: downloads/<domain>/`);
  });
}

async function runCli(cmd, args) {
  ensureFolders();

  if (cmd === "resort-downloads") {
    const dryRun = !args.apply; // default dry-run unless --apply
    const conflict = String(args.conflict || "suffix"); // suffix | skip | overwrite
    const limit = args.limit ? Number(args.limit) : null;

    // Domain selection:
    //  - --domain=electionresults.govt.nz
    //  - or --crawl_root=https://...
    //  - default: "default"
    const dk = safeDomainKey(String(args.domain || "")) || null;
    const dk2 = args.crawl_root ? require("./lib/domain").domainKeyFromUrl(String(args.crawl_root)) : null;
    const domainKey = dk || dk2 || "default";
    const cfg = domainCfg(baseCfg, domainKey);
    ensureDomainFolders(cfg);

    // Optional override: allow targeting a custom downloads root.
    const rootOverride = args.root ? String(args.root) : null;
    await resortDownloads({ cfg, downloadsRootOverride: rootOverride, dryRun, conflict, limit });
    return;
  }

  console.error(`Unknown command: ${cmd}`);
  console.error("Usage:");
  console.error("  node src/index.js resort-downloads [--domain=example.com | --crawl_root=https://example.com/] [--apply] [--root=/path/to/downloads] [--conflict=suffix|skip|overwrite] [--limit=N]");
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
