const express = require("express");
const fs = require("fs");
const path = require("path");

const { ensureDir, readJsonSafe, writeJson } = require("../lib/fsx");
const { appendJsonl } = require("../lib/jsonl");
const { toAbsolute } = require("../lib/paths");

function manifestPath(cfg, level) {
  return path.join(cfg.LEVEL_FILES_DIR, `${String(level)}.json`);
}

function minLevel(levelsObj) {
  const ks = Object.keys(levelsObj || {})
    .map(Number)
    .filter((n) => Number.isFinite(n));
  if (!ks.length) return Infinity;
  return Math.min(...ks);
}

function makeRunsRouter(cfg) {
  const r = express.Router();

  // Hard reset for a BFS file-download level, BUT keep any file also used
  // by an earlier level (< level).
  //
  // Body: { level: number }
  r.post("/runs/start/files", (req, res) => {
    try {
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }
      const L = String(level);

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

        // Remove this level's membership.
        if (rec?.levels && rec.levels[L]) {
          delete rec.levels[L];
          removedLevelRefs++;
        }

        // Keep if any earlier level still references it.
        const earlierMin = rec?.levels ? minLevel(rec.levels) : Infinity;
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
        if (rec && (!rec.levels || Object.keys(rec.levels).length === 0)) {
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
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeRunsRouter };
