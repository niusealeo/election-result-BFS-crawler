const express = require("express");
const path = require("path");
const fs = require("fs");
const { resolveSavePath } = require("../lib/routing");
const { loadElectoratesMeta } = require("../lib/electorates");
const { ensureDir } = require("../lib/fsx");
const { sniffIsPdf, looksLikeHtml } = require("../lib/pdfguard");
const { appendJsonl } = require("../lib/jsonl");

function makeUploadRouter(cfg) {
  const r = express.Router();

  r.post("/upload/file", (req, res) => {
    try {
      const url = req.body?.url;
      const b64 = req.body?.content_base64;
      const ext = req.body?.ext;
      const source_page_url = req.body?.source_page_url || null;

      if (!url || !b64) return res.status(400).json({ ok: false, error: "Missing url or content_base64" });

      const electoratesByTerm = loadElectoratesMeta(cfg.ELECTORATES_BY_TERM_PATH);

      const route = resolveSavePath({
        downloadsRoot: cfg.DOWNLOADS_ROOT,
        url,
        ext,
        source_page_url,
        electoratesByTerm,
      });

      ensureDir(route.finalDir);

      const buf = Buffer.from(String(b64), "base64");

      const shouldBePdf = route.ext === "pdf" || route.filename.toLowerCase().endsWith(".pdf");

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

      appendJsonl(cfg.LOG_FILE_SAVES, {
        ts: new Date().toISOString(),
        url,
        source_page_url: source_page_url || null,
        termKey: route.termKey,
        electorateFolder: route.electorateFolder || null,
        saved_to: outPath,
        bytes: buf.length,
        ext: route.ext,
        note,
      });

      res.json({
        ok: true,
        saved_to: outPath,
        bytes: buf.length,
        termKey: route.termKey,
        electorateFolder: route.electorateFolder || null,
        note,
      });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  return r;
}

module.exports = { makeUploadRouter };
