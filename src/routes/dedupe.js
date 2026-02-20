const express = require("express");
const path = require("path");
const { readJsonSafe, ensureDir, writeJson } = require("../lib/fsx");
const { stableUniqUrls, extFromUrl } = require("../lib/urlnorm");
const { mergeFilesPreferSource } = require("../lib/dedupe");
const { loadState, saveState, computeSeenUpTo } = require("../lib/state");
const { appendJsonl } = require("../lib/jsonl");
const { writeUrlArtifact, writeFileArtifact, writeRowListArtifact } = require("../lib/artifacts");
const { cfgForReq } = require("../lib/domain");
const { logEvent } = require("../lib/logger");

function readUrlArtifactList(p) {
  const raw = readJsonSafe(p, null);
  if (!Array.isArray(raw)) return [];
  return raw
    .map((r) => (typeof r === "string" ? r : r?.url))
    .filter(Boolean);
}

function readFileArtifactList(p) {
  const raw = readJsonSafe(p, null);
  if (!Array.isArray(raw)) return [];
  return raw
    .map((r) => (typeof r === "string" ? { url: r } : r))
    .filter((r) => r && r.url);
}

function diffByUrl(oldArr, newArr) {
  const oldSet = new Set((oldArr || []).map((x) => (typeof x === "string" ? x : x?.url)).filter(Boolean));
  const newSet = new Set((newArr || []).map((x) => (typeof x === "string" ? x : x?.url)).filter(Boolean));

  const added = [];
  const removed = [];

  for (const x of newArr || []) {
    const u = typeof x === "string" ? x : x?.url;
    if (u && !oldSet.has(u)) added.push(x);
  }
  for (const x of oldArr || []) {
    const u = typeof x === "string" ? x : x?.url;
    if (u && !newSet.has(u)) removed.push(x);
  }
  return { added, removed };
}

function makeDedupeRouter(baseCfg) {
  const r = express.Router();

  r.post("/dedupe/level", (req, res) => {
    try {
      const cfg = cfgForReq(baseCfg, req);
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

      // Optional incremental "update" mode:
      // - Diff newly produced artifacts against the existing artifacts on disk
      // - Overwrite artifacts (thereby pruning URLs/files that no longer exist)
      // - Emit diff files that list added/removed URLs/files
      const doUpdateDiff = Boolean(req.body?.update || req.body?.mode === "update");

      let urlsDiffPath = null;
      let filesDiffPath = null;
      let urlsRemovedPathOut = null;
      let filesRemovedPathOut = null;
      let urls_added = 0;
      let urls_removed = 0;
      let files_added = 0;
      let files_removed = 0;

      if (doUpdateDiff) {
        ensureDir(cfg.ARTIFACT_DIR);

        const oldUrls = readUrlArtifactList(nextUrlsPath);
        const oldFiles = readFileArtifactList(filesPath);

        const { added: addedUrls, removed: removedUrls } = diffByUrl(oldUrls, nextPages);
        const { added: addedFiles, removed: removedFiles } = diffByUrl(oldFiles, filesForArtifact);

        urls_added = addedUrls.length;
        urls_removed = removedUrls.length;
        files_added = addedFiles.length;
        files_removed = removedFiles.length;

        urlsDiffPath = path.join(cfg.ARTIFACT_DIR, `urls-diff-level-${nextLevel}.json`);
        filesDiffPath = path.join(cfg.ARTIFACT_DIR, `files-diff-level-${level}.json`);

        // Keep "removed" in separate artifacts.
        // Rationale: Postman runners typically consume diff artifacts as *download queues*.
        // Removed URLs/files are still valuable for reconciliation/audit, but should not
        // appear in the download queue to avoid confusion.
        const urlsRemovedPath = path.join(cfg.ARTIFACT_DIR, `urls-removed-level-${nextLevel}.json`);
        const filesRemovedPath = path.join(cfg.ARTIFACT_DIR, `files-removed-level-${level}.json`);
        urlsRemovedPathOut = urlsRemovedPath;
        filesRemovedPathOut = filesRemovedPath;

        // Download queue rows (added only)
        const urlRows = addedUrls
          .map((u) => ({ url: typeof u === "string" ? u : u?.url, change: "added" }))
          .filter((r) => r.url);

        // Reconciliation rows (removed only)
        const urlRemovedRows = removedUrls
          .map((u) => ({ url: typeof u === "string" ? u : u?.url, change: "removed" }))
          .filter((r) => r.url);

        // Download queue rows (added only)
        const fileRows = addedFiles
          .map((f) => ({
            url: f.url,
            ext: f.ext || extFromUrl(f.url) || "bin",
            source_page_url: f.source_page_url || null,
            change: "added",
          }))
          .filter((r) => r.url);

        // Reconciliation rows (removed only)
        const fileRemovedRows = removedFiles
          .map((f) => ({
            url: f.url,
            ext: f.ext || extFromUrl(f.url) || "bin",
            source_page_url: f.source_page_url || null,
            change: "removed",
          }))
          .filter((r) => r.url);

        writeRowListArtifact({
          path: urlsDiffPath,
          rows: urlRows,
          kind: "urls-diff",
          level: nextLevel,
          metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW,
        });

        writeRowListArtifact({
          path: urlsRemovedPath,
          rows: urlRemovedRows,
          kind: "urls-removed",
          level: nextLevel,
          metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW,
        });

        writeRowListArtifact({
          path: filesDiffPath,
          rows: fileRows,
          kind: "files-diff",
          level,
          metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW,
        });

        writeRowListArtifact({
          path: filesRemovedPath,
          rows: fileRemovedRows,
          kind: "files-removed",
          level,
          metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW,
        });
      }

      writeUrlArtifact({ path: nextUrlsPath, urls: nextPages, nextLevel, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });
      writeFileArtifact({ path: filesPath, files: filesForArtifact, level, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });

      logEvent("DEDUPE_LEVEL", {
        mode: "legacy",
        domain_key: cfg.domain_key,
        level,
        next_level: nextLevel,
        visited: visited.length,
        pages_in: pages.length,
        next_pages: nextPages.length,
        files_out: filesForArtifact.length,
        update: doUpdateDiff ? true : false,
        wrote_next_urls: nextUrlsPath,
        wrote_files: filesPath,
      });

      appendJsonl(cfg.LOG_DEDUPE, {
        ts: new Date().toISOString(),
        level,
        update: doUpdateDiff ? true : false,
        visited: visited.length,
        pages_in: pages.length,
        pages_out_next: nextPages.length,
        files_in: inFiles.length,
        files_merged: filesMerged.length,
        files_out: filesOut.length,
        urls_added,
        urls_removed,
        files_added,
        files_removed,
      });

      res.json({
        ok: true,
        level,
        next_level: nextLevel,
        wrote_next_urls: nextPages.length,
        wrote_files: filesOut.length,
        next_urls_path: nextPages.length ? nextUrlsPath : null,
        files_path: filesOut.length ? filesPath : null,
        update: doUpdateDiff
          ? {
              urls_added,
              urls_removed,
              files_added,
              files_removed,
              urls_diff_path: urlsDiffPath,
              files_diff_path: filesDiffPath,
              urls_removed_path: urlsRemovedPathOut,
              files_removed_path: filesRemovedPathOut,
            }
          : null,
      });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeDedupeRouter };
