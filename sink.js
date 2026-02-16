// sink.js
// Node 18+
// Receives JSON { filename, ext, content_b64, url, status } and overwrites file on disk.

import express from "express";
import fs from "fs";
import path from "path";

const app = express();

// Bump this if you expect large PDFs/ZIPs/XLSX.
// Base64 adds ~33% overhead, so a 30MB file becomes ~40MB JSON payload.
app.use(express.json({ limit: "250mb" }));

const OUT_DIR = path.resolve("./downloads");
fs.mkdirSync(OUT_DIR, { recursive: true });

function safeName(name) {
  // keep it simple: prevent path traversal and illegal chars
  const base = path.basename(String(name || "download.bin"));
  return base.replace(/[<>:"/\\|?*\u0000-\u001F]/g, "_").trim();
}

app.post("/upload", (req, res) => {
  try {
    const { filename, ext, content_b64, url, status } = req.body || {};

    if (!filename || !content_b64) {
      return res.status(400).json({ ok: false, error: "missing filename/content_b64" });
    }

    const safeFile = safeName(filename);
    const safeExt = safeName(String(ext || "bin").toLowerCase());

    const dir = path.join(OUT_DIR, safeExt);
    fs.mkdirSync(dir, { recursive: true });

    const outPath = path.join(dir, safeFile);

    const buf = Buffer.from(content_b64, "base64");

    // OVERWRITE (explicit)
    fs.writeFileSync(outPath, buf, { flag: "w" });

    return res.json({
      ok: true,
      saved: outPath,
      bytes: buf.length,
      status: status ?? null,
      url: url ?? null
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e) });
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Sink listening: http://localhost:${PORT}/upload`);
  console.log(`Writing to: ${OUT_DIR}`);
});
