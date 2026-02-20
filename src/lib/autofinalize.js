const fs = require("fs");
const path = require("path");

const { withLock } = require("./lock");
const { domainCfg, ensureDomainFolders } = require("./domain");
const { logEvent } = require("./logger");

// Streaming discovery run buckets are stored as JSONL files in:
//   BFS_crawl/runs/<domainKey>/discover_level_<level>_<runId>.jsonl
//
// If Postman crashes mid-run, /runs/finalize/urls may never be called.
// This module scans for idle run buckets and finalizes them automatically.

function parseRunFileName(fileName) {
  // discover_level_5_run_123.jsonl
  const m = /^discover_level_(\d+)_(.+)\.jsonl$/i.exec(String(fileName || ""));
  if (!m) return null;
  const level = Number(m[1]);
  const run_id = String(m[2] || "");
  if (!Number.isFinite(level) || level < 1) return null;
  if (!run_id) return null;
  return { level, run_id };
}

function doneMarkerPath(jsonlPath) {
  return `${jsonlPath}.done`;
}

async function startAutoFinalize({ baseCfg, finalizeDiscoveryRun }) {
  if (!baseCfg.AUTO_FINALIZE_ENABLED) return;

  const intervalMs = Math.max(5000, Number(baseCfg.AUTO_FINALIZE_INTERVAL_MS || 60000));
  const idleMs = Math.max(10000, Number(baseCfg.AUTO_FINALIZE_IDLE_MS || 180000));

  logEvent("AUTO_FINALIZE_ENABLED", { interval_ms: intervalMs, idle_ms: idleMs });

  setInterval(() => {
    // Never overlap scans.
    withLock(async () => {
      try {
        const runsRoot = baseCfg.RUNS_ROOT;
        if (!runsRoot || !fs.existsSync(runsRoot)) return;

        const now = Date.now();
        const domains = fs.readdirSync(runsRoot, { withFileTypes: true }).filter(d => d.isDirectory());

        for (const d of domains) {
          const domainKey = d.name;
          const dir = path.join(runsRoot, domainKey);
          let files;
          try {
            files = fs.readdirSync(dir, { withFileTypes: true }).filter(f => f.isFile() && f.name.endsWith(".jsonl"));
          } catch {
            continue;
          }

          for (const f of files) {
            const parsed = parseRunFileName(f.name);
            if (!parsed) continue;
            const jsonlPath = path.join(dir, f.name);
            const donePath = doneMarkerPath(jsonlPath);
            if (fs.existsSync(donePath)) continue;

            let st;
            try { st = fs.statSync(jsonlPath); } catch { continue; }
            if (!st || !st.size) continue;
            const age = now - (st.mtimeMs || st.mtime.getTime());
            if (age < idleMs) continue;

            // Finalize the run into the correct domain.
            const cfg = domainCfg(baseCfg, domainKey);
            ensureDomainFolders(cfg);

            logEvent("AUTO_FINALIZE_TRIGGER", {
              domain_key: domainKey,
              level: parsed.level,
              run_id: parsed.run_id,
              size_bytes: st.size,
              age_ms: Math.round(age),
              jsonl: jsonlPath,
            });

            const result = await finalizeDiscoveryRun({ baseCfg, cfg, level: parsed.level, run_id: parsed.run_id, jsonlPath });
            // Mark as done even if finalize had nothing new; prevents repeated rescans.
            try {
              fs.writeFileSync(donePath, JSON.stringify({ ts: new Date().toISOString(), ...result }, null, 2), { encoding: "utf-8" });
            } catch {}

            logEvent("AUTO_FINALIZE_DONE", {
              domain_key: domainKey,
              level: parsed.level,
              run_id: parsed.run_id,
              visited: result?.visited,
              next_pages: result?.next_pages,
              files: result?.files,
              remaining: result?.remaining,
            });
          }
        }
      } catch (e) {
        logEvent("AUTO_FINALIZE_ERROR", { error: String(e?.message || e) });
      }
    });
  }, intervalMs);
}

module.exports = {
  startAutoFinalize,
  doneMarkerPath,
  parseRunFileName,
};
