const express = require("express");
const fs = require("fs");
const path = require("path");

const { ensureDir, readJsonSafe, writeJson } = require("../lib/fsx");
const { appendJsonl } = require("../lib/jsonl");
const { toAbsolute } = require("../lib/paths");
const { withLock } = require("../lib/lock");
const { cfgForReq } = require("../lib/domain");

function manifestPath(cfg, level) {
  return path.join(cfg.LEVEL_FILES_DIR, `${String(level)}.json`);
}

function levelsFromSources(rec) {
  const out = new Set();
  for (const s of rec?.sources || []) {
    const n = Number(s?.level);
    if (Number.isFinite(n)) out.add(n);
  }
  return [...out].sort((a,b)=>a-b);
}

function minLevelFromSources(rec) {
  const levels = levelsFromSources(rec);
  if (!levels.length) return Infinity;
  return Math.min(...levels);
}

function makeRunsRouter(baseCfg) {
  const r = express.Router();

  // Hard reset for a BFS file-download level, BUT keep any file also used
  // by an earlier level (< level).
  //
  // Body: { level: number }
  r.post("/runs/start/files", async (req, res) => {
    try {
      const cfg = cfgForReq(baseCfg, req);
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }
      return await withLock(() => {
        ensureDir(cfg.LEVEL_FILES_DIR);

      const mPath = manifestPath(cfg, level);
      const manifest = readJsonSafe(mPath, { level, files: [] });

      const idx = readJsonSafe(cfg.DOWNLOADED_HASH_INDEX_PATH, {});

      let deletedFiles = 0;
      let keptBecauseEarlier = 0;
      let missingFiles = 0;
      let removedLevelRefs = 0;
      let deletedHashes = 0;

      for (const item of manifest.files || []) {
        const sha = item?.sha256;
        const savedRel = item?.saved_to;

        const rec = sha ? idx[sha] : null;

        // Remove this level's membership by dropping matching source observations.
        if (rec?.sources && Array.isArray(rec.sources)) {
          const before = rec.sources.length;
          rec.sources = rec.sources.filter((s) => Number(s?.level) !== level);
          if (rec.sources.length !== before) removedLevelRefs += (before - rec.sources.length);
        }

        // Keep if any earlier level still references it.
        const earlierMin = rec ? minLevelFromSources(rec) : Infinity;
        const usedByEarlier = earlierMin < level;
        if (usedByEarlier) {
          keptBecauseEarlier++;
          continue;
        }

        if (savedRel) {
          const savedAbs = toAbsolute(savedRel);
          if (fs.existsSync(savedAbs)) {
            try {
              fs.unlinkSync(savedAbs);
              deletedFiles++;
            } catch {
              // best-effort
            }
          } else {
            missingFiles++;
          }
        }

        // If no remaining levels, drop the hash record.
        // If no remaining sources, drop the hash record.
        if (rec && (!rec.sources || rec.sources.length === 0)) {
          delete idx[sha];
          deletedHashes++;
        } else if (rec) {
          rec.last_seen_ts = new Date().toISOString();
        }
      }

      // Overwrite manifest for this level as fresh.
      writeJson(mPath, { level, files: [], started_ts: new Date().toISOString() });
      writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);

      appendJsonl(cfg.LOG_LEVEL_RESETS, {
        ts: new Date().toISOString(),
        kind: "files",
        level,
        deletedFiles,
        keptBecauseEarlier,
        missingFiles,
        removedLevelRefs,
        deletedHashes,
      });

      return res.json({
        ok: true,
        level,
        deletedFiles,
        keptBecauseEarlier,
        missingFiles,
        removedLevelRefs,
        deletedHashes,
      });
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeRunsRouter };
