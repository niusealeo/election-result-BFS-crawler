const express = require("express");
const path = require("path");
const { stableUniqUrls, extFromUrl } = require("../lib/urlnorm");
const { mergeFilesPreferSource } = require("../lib/dedupe");
const { loadState, saveState, computeSeenUpTo } = require("../lib/state");
const { appendJsonl } = require("../lib/jsonl");
const { writeUrlArtifact, writeFileArtifact } = require("../lib/artifacts");

function makeDedupeRouter(cfg) {
  const r = express.Router();

  r.post("/dedupe/level", (req, res) => {
    try {
      const level = Number(req.body?.level);
      if (!Number.isFinite(level) || level < 1) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }

      const extractUrlArray = (raw) =>
        stableUniqUrls(
          (Array.isArray(raw) ? raw : [])
            .map((r) => (typeof r === "string" ? r : r?.url))
            .filter(Boolean)
        );

      const visited = extractUrlArray(req.body?.visited || []);
      const pages = extractUrlArray(req.body?.pages || []);

      const inFiles = Array.isArray(req.body?.files) ? req.body.files : [];
      const filesMerged = mergeFilesPreferSource(inFiles);

      const st = loadState(cfg.STATE_PATH);
      st.levels[String(level)] = { visited, pages, files: filesMerged };
      saveState(cfg.STATE_PATH, st);

      const { seenPages: seenPagesBefore, seenFiles: seenFilesBefore } = computeSeenUpTo(st, level - 1);
      const seenForNextPages = new Set([...seenPagesBefore, ...visited]);

      const nextPages = pages.filter((u) => !seenForNextPages.has(u));
      const filesOut = filesMerged.filter((f) => !seenFilesBefore.has(f.url));

      const filesForArtifact = filesOut.map((f) => ({
        url: f.url,
        ext: (f.ext || extFromUrl(f.url) || "bin").toLowerCase(),
        source_page_url: f.source_page_url || null,
      }));

      const nextLevel = level + 1;
      const nextUrlsPath = path.join(cfg.ARTIFACT_DIR, `urls-level-${nextLevel}.json`);
      const filesPath = path.join(cfg.ARTIFACT_DIR, `files-level-${level}.json`);

      writeUrlArtifact({ path: nextUrlsPath, urls: nextPages, nextLevel, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });
      writeFileArtifact({ path: filesPath, files: filesForArtifact, level, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });

      appendJsonl(cfg.LOG_DEDUPE, {
        ts: new Date().toISOString(),
        level,
        visited: visited.length,
        pages_in: pages.length,
        pages_out_next: nextPages.length,
        files_in: inFiles.length,
        files_merged: filesMerged.length,
        files_out: filesOut.length,
      });

      res.json({
        ok: true,
        level,
        next_level: nextLevel,
        wrote_next_urls: nextPages.length,
        wrote_files: filesOut.length,
        next_urls_path: nextPages.length ? nextUrlsPath : null,
        files_path: filesOut.length ? filesPath : null,
      });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeDedupeRouter };
