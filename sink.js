// sink.js
// Node 18+
// Saves raw bytes POSTed to /save into ./downloads/<ext>/<filename>

import http from "http";
import fs from "fs";
import path from "path";

const PORT = 3000;
const OUT = path.resolve("./downloads");

function safeName(name) {
  return name.replace(/[<>:"/\\|?*\u0000-\u001F]/g, "_").trim();
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

http.createServer((req, res) => {
  if (req.method !== "POST" || req.url !== "/save") {
    res.writeHead(404); return res.end("Not found");
  }

  const filename = safeName(decodeURIComponent(req.headers["x-filename"] || "download.bin"));
  const ext = safeName(String(req.headers["x-ext"] || "bin").toLowerCase());
  const dir = path.join(OUT, ext);
  ensureDir(dir);

  const outPath = path.join(dir, filename);
  const ws = fs.createWriteStream(outPath);

  req.pipe(ws);

  ws.on("finish", () => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end(`Saved ${outPath}`);
  });
  ws.on("error", (e) => {
    res.writeHead(500); res.end(String(e));
  });
}).listen(PORT, () => {
  console.log(`Sink listening on http://localhost:${PORT}/save`);
  console.log(`Writing to ${OUT}`);
});
