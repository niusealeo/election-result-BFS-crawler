const express = require("express");
const path = require("path");

const { ensureDir, readJsonSafe, writeJson } = require("../lib/fsx");
const { appendJsonl } = require("../lib/jsonl");
const { writeRowListArtifact } = require("../lib/artifacts");

function inferExtFromUrl(url) {
  try {
    const u = new URL(url);
    const p = u.pathname || "";
    const m = p.match(/\.([a-zA-Z0-9]{1,8})$/);
    return m ? m[1].toLowerCase() : null;
  } catch {
    const m = String(url || "").match(/\.([a-zA-Z0-9]{1,8})(?:\?|#|$)/);
    return m ? m[1].toLowerCase() : null;
  }
}

function pickSignature(rec) {
  // Prefer HEAD if it looks useful; otherwise fall back to GET-range.
  const h = rec?.head || null;
  const g = rec?.get_range || null;

  function sigFrom(x) {
    if (!x) return null;
    return {
      etag: x.etag || null,
      last_modified: x.last_modified || null,
      content_length: Number.isFinite(x.content_length) ? x.content_length : null,
      content_type: x.content_type || null,
    };
  }

  const hs = sigFrom(h);
  const gs = sigFrom(g);

  const headUseful = hs && (hs.etag || hs.last_modified || hs.content_length);
  const getUseful = gs && (gs.etag || gs.last_modified || gs.content_length);

  if (headUseful) return { source: "head", ...hs };
  if (getUseful) return { source: "get_range", ...gs };
  return { source: h ? "head" : "get_range", ...(hs || gs || {}) };
}

function sigChanged(a, b) {
  if (!a && !b) return false;
  if (!a || !b) return true;
  return (
    (a.etag || null) !== (b.etag || null) ||
    (a.last_modified || null) !== (b.last_modified || null) ||
    (Number.isFinite(a.content_length) ? a.content_length : null) !== (Number.isFinite(b.content_length) ? b.content_length : null) ||
    (a.content_type || null) !== (b.content_type || null)
  );
}

function loadFilesLevelContext({ cfg, level }) {
  // IMPORTANT: artifacts use a conflated first row: row[0] is a real data row
  // that ALSO carries shared/meta attributes. Therefore rows 0..N are data rows.
  const filesPath = path.join(cfg.ARTIFACT_DIR, `files-level-${level}.json`);
  const rows = readJsonSafe(filesPath, null);
  if (!Array.isArray(rows) || rows.length === 0) {
    return { row0: null, byUrl: new Map() };
  }

  const row0 = rows[0] || null;
  const byUrl = new Map();
  for (const r of rows) {
    const u = r && typeof r === "object" ? r.url : null;
    if (u) byUrl.set(String(u), r);
  }
  return { row0, byUrl };
}

function resolveField(row, row0, key) {
  if (row && row[key] !== undefined && row[key] !== null) return row[key];
  if (row0 && row0[key] !== undefined && row0[key] !== null) return row0[key];
  return null;
}

function makeProbeRouter(cfg) {
  const r = express.Router();

  // POST /probe/meta
  // Body should include:
  //  - url
  //  - level (optional)
  //  - crawl_root (optional)
  //  - head: {etag,last_modified,content_length,...}
  //  - get_range: {etag,last_modified,content_length,content_range,range_honoured,...}
  //
  // This endpoint:
  //  - appends raw record to jsonl log
  //  - updates cfg.PROBE_META_INDEX_PATH (per-url latest signature)
  //  - emits files-meta-diff-level-L.json when a change is detected (for Postman Runner)
  r.post("/probe/meta", (req, res) => {
    try {
      const body = req.body || {};
      const url = body.url ? String(body.url) : null;
      const level = body.level != null && body.level !== "" ? Number(body.level) : null;

      if (!url) return res.status(400).json({ ok: false, error: "Missing url" });
      if (level != null && (!Number.isFinite(level) || level < 1)) {
        return res.status(400).json({ ok: false, error: "Invalid level" });
      }

      // Persist raw probe record
      appendJsonl(cfg.LOG_META_PROBES, { ts: new Date().toISOString(), ...body, url, level });

      // Update latest signature index
      ensureDir(cfg.META_DIR);
      const idx = readJsonSafe(cfg.PROBE_META_INDEX_PATH, {});
      const prev = idx[url] || null;

      const sig = pickSignature(body);
      const prevSig = prev?.signature || null;

      const changed = sigChanged(prevSig, sig);
      idx[url] = {
        url,
        last_seen_ts: new Date().toISOString(),
        level: level || prev?.level || null,
        signature: sig,
        head: body.head || null,
        get_range: body.get_range || null,
      };
      writeJson(cfg.PROBE_META_INDEX_PATH, idx);

      // If level provided and changed, emit/append to per-level modified artifact
      let diffPath = null;
      if (changed && level) {
        const { row0: filesRow0, byUrl: filesByUrl } = loadFilesLevelContext({ cfg, level });
        const ctxRow = filesByUrl.get(url) || null;
        const ctxExt = resolveField(ctxRow, filesRow0, "ext") || inferExtFromUrl(url);
        const ctxSourcePageUrl = resolveField(ctxRow, filesRow0, "source_page_url");

        ensureDir(cfg.ARTIFACT_DIR);
        diffPath = path.join(cfg.ARTIFACT_DIR, `files-meta-diff-level-${level}.json`);

        // Keep a unique set of modified urls in the artifact.
        const existing = readJsonSafe(diffPath, null);
        const rows = [];
        if (Array.isArray(existing)) {
          for (const r0 of existing) {
            const u = typeof r0 === "string" ? r0 : r0?.url;
            if (u) rows.push({ url: u, change: r0?.change || "modified" });
          }
        }

        const seen = new Set(rows.map((r) => r.url));
        if (!seen.has(url)) rows.push({ url, change: "modified" });

        writeRowListArtifact({
          path: diffPath,
          rows,
          kind: "files-meta-diff",
          level,
          metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW,
          extraMeta: { source: "probe-meta" },
        });

        // ALSO: merge modified URLs into the download-queue diff artifact for this level.
        // Postman Step 3 should be able to consume files-diff-level-L.json directly.
        const dlDiffPath = path.join(cfg.ARTIFACT_DIR, `files-diff-level-${level}.json`);
        const existingDl = readJsonSafe(dlDiffPath, null);
        const dlRows = [];

        if (Array.isArray(existingDl)) {
          for (const r0 of existingDl) {
            if (!r0) continue;
            const u = typeof r0 === "string" ? r0 : r0.url;
            if (!u) continue;
            dlRows.push({
              url: u,
              change: r0.change || "added",
              ext: r0.ext || inferExtFromUrl(u),
              source_page_url: r0.source_page_url || null,
            });
          }
        }

        // If URL is already queued as "added" or "modified", don't add another row.
        const alreadyQueued = new Set(dlRows.map((r) => r.url));
        if (!alreadyQueued.has(url)) {
          dlRows.push({ url, change: "modified", ext: ctxExt, source_page_url: ctxSourcePageUrl });
        }

        writeRowListArtifact({
          path: dlDiffPath,
          rows: dlRows,
          kind: "files-diff",
          level,
          metaFirstRow: cfg.ARTIFACT_META_FIRST_ROW,
          extraMeta: { source: "dedupe+probe" },
        });
      }

      return res.json({ ok: true, url, level, changed, diff_path: diffPath });
    } catch (e) {
      return res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeProbeRouter };
