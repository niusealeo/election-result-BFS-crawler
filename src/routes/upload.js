const express = require("express");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");

const { resolveSavePath } = require("../lib/routing");
const { loadElectoratesMeta } = require("../lib/electorates");
const { ensureDir, readJsonSafe, writeJson } = require("../lib/fsx");
const { sniffIsPdf, looksLikeHtml } = require("../lib/pdfguard");
const { appendJsonl } = require("../lib/jsonl");
const { normalizeUrl } = require("../lib/urlnorm");

function uniqPush(arr, v, limit = 2000) {
  if (!v) return arr || [];
  if (!Array.isArray(arr)) arr = [];
  if (!arr.includes(v)) arr.push(v);
  if (arr.length > limit) arr = arr.slice(arr.length - limit);
  return arr;
}

function levelKey(level) {
  const n = Number(level);
  return Number.isFinite(n) ? String(n) : null;
}

function isEarlier(levelA, orderA, levelB, orderB) {
  // Earlier BFS means lower level; tie-breaker is lower order.
  if (levelA < levelB) return true;
  if (levelA > levelB) return false;
  return orderA < orderB;
}

function safeMove(oldPath, newPath) {
  if (oldPath === newPath) return;
  ensureDir(path.dirname(newPath));
  try {
    fs.renameSync(oldPath, newPath);
  } catch {
    // cross-device or permission fallback: copy then unlink
    fs.copyFileSync(oldPath, newPath);
    try { fs.unlinkSync(oldPath); } catch {}
  }
}

function makeUploadRouter(cfg) {
  const r = express.Router();

  r.post("/upload/file", (req, res) => {
    try {
      const url = req.body?.url;
      const b64 = req.body?.content_base64;
      const ext = req.body?.ext;
      const filenameOverride = req.body?.filename ? String(req.body.filename) : null;
      const source_page_url = req.body?.source_page_url ? String(req.body.source_page_url) : null;

      // NEW: BFS metadata from Postman
      const bfs_level_raw = req.body?.bfs_level;
      const bfs_order_raw = req.body?.bfs_order;

      const bfs_level = Number(bfs_level_raw);
      const bfs_order = Number(bfs_order_raw);

      const bfsLevelOk = Number.isFinite(bfs_level) && bfs_level >= 1;
      const bfsOrderOk = Number.isFinite(bfs_order) && bfs_order >= 0;

      if (!url || !b64) return res.status(400).json({ ok: false, error: "Missing url or content_base64" });
      if (!bfsLevelOk || !bfsOrderOk) {
        return res.status(400).json({ ok: false, error: "Missing/invalid bfs_level or bfs_order" });
      }

      const normUrl = normalizeUrl(url);
      const normSource = source_page_url ? normalizeUrl(source_page_url) : null;

      // Decode bytes + hash (dedupe key is content)
      const buf = Buffer.from(String(b64), "base64");
      const sha256 = crypto.createHash("sha256").update(buf).digest("hex");

      // Load index: sha256 -> canonical record
      const idx = readJsonSafe(cfg.DOWNLOADED_HASH_INDEX_PATH, {});
      const prev = idx[sha256];

      // Decide the desired save path for THIS appearance (used if it becomes primary)
      const electoratesByTerm = loadElectoratesMeta(cfg.ELECTORATES_BY_TERM_PATH);
      const route = resolveSavePath({
        downloadsRoot: cfg.DOWNLOADS_ROOT,
        url,
        ext,
        source_page_url,
        electoratesByTerm,
        filenameOverride,
      });

      const shouldBePdf = route.ext === "pdf" || route.filename.toLowerCase().endsWith(".pdf");

      // If already have this hash saved somewhere, update provenance + maybe promote if earlier
      if (prev && prev.saved_to && fs.existsSync(prev.saved_to)) {
        prev.last_seen_ts = new Date().toISOString();
        prev.urls = uniqPush(prev.urls, normUrl);
        prev.sources = uniqPush(prev.sources, normSource);
        prev.names = uniqPush(prev.names, filenameOverride || route.filename);
        prev.levels = prev.levels || {};
        prev.levels[String(bfs_level)] = true;

        const primary_level = Number(prev.primary_level ?? Infinity);
        const primary_order = Number(prev.primary_order ?? Infinity);

        // Promote if this appearance is earlier than the current primary
        if (isEarlier(bfs_level, bfs_order, primary_level, primary_order)) {
          // Move canonical file to the new earlier location (so “priority = earliest BFS”)
          // Keep PDF quarantine logic consistent: if this is a PDF but bytes aren't PDF, we quarantine.
          let targetPath = route.outPath;
          let note = "promoted_ok";

          if (shouldBePdf && !sniffIsPdf(buf)) {
            note = looksLikeHtml(buf) ? "promoted_bad_pdf_got_html" : "promoted_bad_pdf_not_pdf";
            const badDir = path.join(route.termDir, "_bad");
            ensureDir(badDir);
            const base = route.filename.replace(/\.pdf$/i, "");
            const badName = `${base}__${note}.html`.replace(/[\/\\]/g, "_");
            targetPath = path.join(badDir, badName);
          }

          safeMove(prev.saved_to, targetPath);
          prev.saved_to = targetPath;
          prev.ext = route.ext;
          prev.termKey = route.termKey;
          prev.electorateFolder = route.electorateFolder || null;
          prev.note = note;
          prev.primary_level = bfs_level;
          prev.primary_order = bfs_order;

          writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);

          appendJsonl(cfg.LOG_FILE_SAVES, {
            ts: new Date().toISOString(),
            url: normUrl,
            source_page_url: normSource,
            saved_to: prev.saved_to,
            bytes: buf.length,
            ext: prev.ext,
            note,
            sha256,
            bfs_level,
            bfs_order,
          });

          return res.json({
            ok: true,
            skipped: true,
            promoted: true,
            note,
            saved_to: prev.saved_to,
            bytes: buf.length,
            sha256,
            bfs_level,
            bfs_order,
          });
        }

        // Not earlier => skip write
        writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);

        appendJsonl(cfg.LOG_FILE_SAVES, {
          ts: new Date().toISOString(),
          url: normUrl,
          source_page_url: normSource,
          saved_to: prev.saved_to,
          bytes: buf.length,
          ext: prev.ext || route.ext,
          note: "duplicate_skipped",
          sha256,
          bfs_level,
          bfs_order,
        });

        return res.json({
          ok: true,
          skipped: true,
          promoted: false,
          note: "duplicate_skipped",
          saved_to: prev.saved_to,
          bytes: buf.length,
          sha256,
          bfs_level,
          bfs_order,
        });
      }

      // Otherwise: save as new canonical (for now)
      ensureDir(route.finalDir);

      let outPath = route.outPath;
      let note = "ok";

      if (shouldBePdf && !sniffIsPdf(buf)) {
        note = looksLikeHtml(buf) ? "bad_pdf_got_html" : "bad_pdf_not_pdf";
        const badDir = path.join(route.termDir, "_bad");
        ensureDir(badDir);

        const base = route.filename.replace(/\.pdf$/i, "");
        const badName = `${base}__${note}.html`.replace(/[\/\\]/g, "_");
        outPath = path.join(badDir, badName);
      }

      fs.writeFileSync(outPath, buf);

      idx[sha256] = {
        sha256,
        bytes: buf.length,
        ext: route.ext,
        saved_to: outPath,
        termKey: route.termKey,
        electorateFolder: route.electorateFolder || null,
        first_seen_ts: prev?.first_seen_ts || new Date().toISOString(),
        last_seen_ts: new Date().toISOString(),
        urls: uniqPush(prev?.urls, normUrl),
        sources: uniqPush(prev?.sources, normSource),
        names: uniqPush(prev?.names, filenameOverride || route.filename),
        levels: { [String(bfs_level)]: true },
        primary_level: bfs_level,
        primary_order: bfs_order,
        note,
      };

      writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);

      appendJsonl(cfg.LOG_FILE_SAVES, {
        ts: new Date().toISOString(),
        url: normUrl,
        source_page_url: normSource,
        termKey: route.termKey,
        electorateFolder: route.electorateFolder || null,
        saved_to: outPath,
        bytes: buf.length,
        ext: route.ext,
        note,
        sha256,
        bfs_level,
        bfs_order,
      });

      return res.json({
        ok: true,
        saved_to: outPath,
        bytes: buf.length,
        sha256,
        termKey: route.termKey,
        electorateFolder: route.electorateFolder || null,
        note,
        bfs_level,
        bfs_order,
      });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeUploadRouter };
