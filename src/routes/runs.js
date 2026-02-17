const express = require("express");
const fs = require("fs");

const { readJsonSafe, writeJson } = require("../lib/fsx");
const { appendJsonl } = require("../lib/jsonl");

function makeRunsRouter(cfg) {
  const r = express.Router();

  // POST /runs/start/files
  // Body: { level: <number> }
  // Clears prior collected results *for that level* from the hash index.
  r.post("/runs/start/files", (req, res) => {
    try {
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }
      const L = String(level);

      const idx = readJsonSafe(cfg.DOWNLOADED_HASH_INDEX_PATH, {});
      let removedLevelRefs = 0;
      let deletedFiles = 0;
      let deletedHashes = 0;

      for (const [sha, rec] of Object.entries(idx)) {
        if (!rec || typeof rec !== "object") continue;
        if (!rec.levels || !rec.levels[L]) continue;

        delete rec.levels[L];
        removedLevelRefs++;

        // recompute primary_level/primary_order if needed
        const levels = Object.keys(rec.levels || {}).map(Number).filter(Number.isFinite);
        if (levels.length === 0) {
          // no remaining references => delete file + delete hash entry
          if (rec.saved_to && fs.existsSync(rec.saved_to)) {
            try { fs.unlinkSync(rec.saved_to); deletedFiles++; } catch {}
          }
          delete idx[sha];
          deletedHashes++;
          continue;
        }

        // if the primary level was this level, we no longer know the true earliest order;
        // keep primary_level as min remaining, and primary_order as Infinity (will be corrected on next upload).
        const newPrimary = Math.min(...levels);
        if (Number(rec.primary_level) === level) {
          rec.primary_level = newPrimary;
          rec.primary_order = Number.POSITIVE_INFINITY;
        }

        rec.last_seen_ts = new Date().toISOString();
      }

      writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);

      appendJsonl(cfg.LOG_LEVEL_RESETS, {
        ts: new Date().toISOString(),
        kind: "files",
        level,
        removedLevelRefs,
        deletedFiles,
        deletedHashes,
      });

      return res.json({
        ok: true,
        level,
        removedLevelRefs,
        deletedFiles,
        deletedHashes,
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeRunsRouter };
