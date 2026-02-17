const express = require("express");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");

const { resolveSavePath } = require("../lib/routing");
const { loadElectoratesMeta, ensureTermElectorateFolders } = require("../lib/electorates");
const { ensureDir, readJsonSafe, writeJson } = require("../lib/fsx");
const { sniffIsPdf, looksLikeHtml } = require("../lib/pdfguard");
const { appendJsonl } = require("../lib/jsonl");
const { toAbsolute, toRelative } = require("../lib/paths");

function manifestPath(cfg, level) {
  return path.join(cfg.LEVEL_FILES_DIR, `${String(level)}.json`);
}

function appendToLevelManifest(cfg, level, entry) {
  ensureDir(cfg.LEVEL_FILES_DIR);
  const p = manifestPath(cfg, level);
  const m = readJsonSafe(p, { level, files: [] });
  const key = `${entry.sha256}::${entry.saved_to}`;
  const seen = new Set((m.files || []).map((x) => `${x.sha256}::${x.saved_to}`));
  if (!seen.has(key)) {
    m.files.push(entry);
    writeJson(p, m);
  }
}

function makeUploadRouter(cfg) {
  const r = express.Router();

  // POST /upload/file
  // Body:
  //  - url
  //  - ext
  //  - filename (optional override)
  //  - source_page_url (optional)
  //  - bfs_level (required)
  //  - content_base64 (required)
  r.post("/upload/file", async (req, res) => {
    try {
      const url = req.body?.url;
      const b64 = req.body?.content_base64;
      const ext = req.body?.ext;
      const filenameOverride = req.body?.filename ? String(req.body.filename) : null;
      const source_page_url = req.body?.source_page_url ? String(req.body.source_page_url) : null;
      const bfs_level = Number(req.body?.bfs_level);

      if (!Number.isFinite(bfs_level) || bfs_level < 1) {
        return res.status(400).json({ ok: false, error: "Missing/invalid bfs_level" });
      }
      if (!url || !b64) {
        return res.status(400).json({ ok: false, error: "Missing url or content_base64" });
      }

      const electoratesByTerm = loadElectoratesMeta(cfg.ELECTORATES_BY_TERM_PATH);

      // Decode bytes + hash by content
      const buf = Buffer.from(String(b64), "base64");
      const sha256 = crypto.createHash("sha256").update(buf).digest("hex");

      // ---- Serialize RMW state updates (hash index + manifests) ----
      return await withLock(() => {
      // Load global hash index (stores relative paths)
      const idx = readJsonSafe(cfg.DOWNLOADED_HASH_INDEX_PATH, {});
      const existing = idx[sha256];

      // Resolve intended save path for THIS occurrence
      const route = resolveSavePath({
        downloadsRoot: cfg.DOWNLOADS_ROOT,
        url,
        ext,
        source_page_url,
        electoratesByTerm,
        filenameOverride,
      });

      // Ensure canonical electorate folders exist for the term
      if (route.termKey && route.termKey !== "term_unknown") {
        ensureTermElectorateFolders({
          downloadsRoot: cfg.DOWNLOADS_ROOT,
          termKey: route.termKey,
          electoratesByTerm,
        });
      }

      const shouldBePdf = route.ext === "pdf" || route.filename.toLowerCase().endsWith(".pdf");

      // If already saved, just mark membership for this level and record in manifest.
      if (existing?.saved_to) {
        const existingAbs = toAbsolute(existing.saved_to);
        if (fs.existsSync(existingAbs)) {
          existing.levels = existing.levels || {};
          existing.levels[String(bfs_level)] = true;
          existing.last_seen_ts = new Date().toISOString();

          // If we can now route it into an electorate folder whereas it previously lived in term root,
          // upgrade location (more specific beats less specific).
          // We ONLY upgrade if the new target is more specific (has electorateFolder) and
          // the existing saved_to is NOT already inside that electorate folder.
          const wantElect = !!route.electorateFolder;
          const haveElect = existing.electorateFolder ? true : false;
          if (wantElect && !haveElect) {
            // Move canonical file to new location.
            let targetAbs = route.outPath;
            let note = "promoted_to_electorate";
            if (shouldBePdf && !sniffIsPdf(buf)) {
              note = looksLikeHtml(buf) ? "promoted_bad_pdf_got_html" : "promoted_bad_pdf_not_pdf";
              const badDir = path.join(route.termDir, "_bad");
              ensureDir(badDir);
              const base = route.filename.replace(/\.pdf$/i, "");
              const badName = `${base}__${note}.html`.replace(/[\/\\]/g, "_");
              targetAbs = path.join(badDir, badName);
            }

            ensureDir(path.dirname(targetAbs));
            try {
              fs.renameSync(existingAbs, targetAbs);
            } catch {
              fs.copyFileSync(existingAbs, targetAbs);
              try { fs.unlinkSync(existingAbs); } catch {}
            }

            existing.saved_to = toRelative(targetAbs);
            existing.termKey = route.termKey;
            existing.electorateFolder = route.electorateFolder || null;
            existing.ext = route.ext;
            existing.note = note;
          }

          idx[sha256] = existing;
          writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);

          appendToLevelManifest(cfg, bfs_level, { sha256, saved_to: existing.saved_to });

          appendJsonl(cfg.LOG_FILE_SAVES, {
            ts: new Date().toISOString(),
            url,
            source_page_url,
            termKey: existing.termKey || route.termKey,
            electorateFolder: existing.electorateFolder || route.electorateFolder || null,
            saved_to: existing.saved_to,
            bytes: buf.length,
            ext: existing.ext || route.ext,
            note: "duplicate_content_skipped",
            sha256,
            bfs_level,
          });

          return res.json({
            ok: true,
            skipped: true,
            note: "duplicate_content_skipped",
            saved_to: existing.saved_to,
            sha256,
          });
        }
        // If record exists but file missing, fall through and re-save.
      }

      // Save new canonical file
      ensureDir(route.finalDir);

      let outAbs = route.outPath;
      let note = "ok";
      if (shouldBePdf && !sniffIsPdf(buf)) {
        note = looksLikeHtml(buf) ? "bad_pdf_got_html" : "bad_pdf_not_pdf";
        const badDir = path.join(route.termDir, "_bad");
        ensureDir(badDir);
        const base = route.filename.replace(/\.pdf$/i, "");
        const badName = `${base}__${note}.html`.replace(/[\/\\]/g, "_");
        outAbs = path.join(badDir, badName);
      }

      fs.writeFileSync(outAbs, buf);
      const outRel = toRelative(outAbs);

      idx[sha256] = {
        sha256,
        saved_to: outRel,
        bytes: buf.length,
        ext: route.ext,
        termKey: route.termKey,
        electorateFolder: route.electorateFolder || null,
        url,
        source_page_url,
        last_seen_ts: new Date().toISOString(),
        levels: { [String(bfs_level)]: true },
        note,
      };
      writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);

      appendToLevelManifest(cfg, bfs_level, { sha256, saved_to: outRel });

      appendJsonl(cfg.LOG_FILE_SAVES, {
        ts: new Date().toISOString(),
        url,
        source_page_url,
        termKey: route.termKey,
        electorateFolder: route.electorateFolder || null,
        saved_to: outRel,
        bytes: buf.length,
        ext: route.ext,
        note,
        sha256,
        bfs_level,
      });

      return res.json({
        ok: true,
        saved_to: outRel,
        bytes: buf.length,
        termKey: route.termKey,
        electorateFolder: route.electorateFolder || null,
        note,
        sha256,
      });
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeUploadRouter };