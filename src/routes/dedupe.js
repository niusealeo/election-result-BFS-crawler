const express = require("express");
const path = require("path");
const { stableUniqUrls, extFromUrl } = require("../lib/urlnorm");
const { mergeFilesPreferSource } = require("../lib/dedupe");
const { loadState, saveState, computeSeenUpTo } = require("../lib/state");
const { appendJsonl } = require("../lib/jsonl");
const { writeUrlArtifact, writeFileArtifact } = require("../lib/artifacts");
const { readJsonSafe, writeJson, ensureDir } = require("../lib/fsx");
const { loadUrlSigIndex, getUrlSig } = require("../lib/urlsig");

function extractUrlsFromArtifact(arr) {
  if (!Array.isArray(arr)) return [];
  return stableUniqUrls(
    arr
      .map((x) => (typeof x === "string" ? x : x?.url))
      .filter(Boolean)
  );
}

function diffSets(prevArr, currArr) {
  const prev = new Set(prevArr || []);
  const curr = new Set(currArr || []);
  const added = [];
  const removed = [];
  for (const u of curr) if (!prev.has(u)) added.push(u);
  for (const u of prev) if (!curr.has(u)) removed.push(u);
  return { added, removed };
}

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

      const update = !!req.body?.update;

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

      // Update-mode diff output paths
      const urlsDiffPath = path.join(cfg.ARTIFACT_DIR, `urls-diff-level-${nextLevel}.json`);
      const filesDiffPath = path.join(cfg.ARTIFACT_DIR, `files-diff-level-${level}.json`);
      const filesSigPath = path.join(cfg.ARTIFACT_DIR, `files-sig-level-${level}.json`);

      writeUrlArtifact({ path: nextUrlsPath, urls: nextPages, nextLevel, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });
      writeFileArtifact({ path: filesPath, files: filesForArtifact, level, metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW });

      let wrote_urls_diff = false;
      let wrote_files_diff = false;
      let modified_count = 0;

      if (update) {
        // Diff against previous artifacts on disk (previous run baseline)
        const prevNextUrls = extractUrlsFromArtifact(readJsonSafe(nextUrlsPath, []));
        const prevFiles = extractUrlsFromArtifact(readJsonSafe(filesPath, []));

        const { added: urls_added, removed: urls_removed } = diffSets(prevNextUrls, nextPages);
        const { added: files_added, removed: files_removed } = diffSets(prevFiles, filesForArtifact.map((f) => f.url));

        // Detect modified files using per-URL signatures (sha256) when available.
        // This requires that the URL has been downloaded at least once in the past.
        const urlSigIdx = loadUrlSigIndex(cfg.URL_SIGNATURE_INDEX_PATH);

        const prevSig = readJsonSafe(filesSigPath, { level, sig: {} });
        const prevSigMap = prevSig?.sig && typeof prevSig.sig === "object" ? prevSig.sig : {};

        const currSigMap = {};
        for (const f of filesForArtifact) {
          const rec = getUrlSig(urlSigIdx, f.url);
          if (rec?.sha256) {
            currSigMap[f.url] = { sha256: rec.sha256, bytes: rec.bytes || null, last_seen_ts: rec.last_seen_ts || null };
          }
        }

        const modified = [];
        for (const u of Object.keys(currSigMap)) {
          const a = prevSigMap[u];
          const b = currSigMap[u];
          if (a?.sha256 && b?.sha256 && a.sha256 !== b.sha256) {
            modified.push({
              url: u,
              from: { sha256: a.sha256, bytes: a.bytes || null },
              to: { sha256: b.sha256, bytes: b.bytes || null },
            });
          }
        }
        modified_count = modified.length;

        // Persist current signatures for next update diff.
        ensureDir(cfg.ARTIFACT_DIR);
        writeJson(filesSigPath, {
          _meta: true,
          kind: "files-sig",
          level,
          ts: new Date().toISOString(),
          sig: currSigMap,
        });

        writeJson(urlsDiffPath, {
          _meta: true,
          kind: "urls-diff",
          level: nextLevel,
          ts: new Date().toISOString(),
          added: urls_added,
          removed: urls_removed,
        });
        wrote_urls_diff = true;

        writeJson(filesDiffPath, {
          _meta: true,
          kind: "files-diff",
          level,
          ts: new Date().toISOString(),
          added: files_added,
          removed: files_removed,
          modified,
        });
        wrote_files_diff = true;
      }

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
        update,
        urls_diff_path: update && wrote_urls_diff ? urlsDiffPath : null,
        files_diff_path: update && wrote_files_diff ? filesDiffPath : null,
        modified_count: update ? modified_count : null,
      });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeDedupeRouter };
