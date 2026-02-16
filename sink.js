// sink.js
// npm i express multer sanitize-filename
// node sink.js

const express = require("express");
const fs = require("fs");
const path = require("path");
const multer = require("multer");
const sanitize = require("sanitize-filename");

const app = express();
app.use(express.json({ limit: "50mb" })); // for meta posts / optional base64 uploads

// ---------------------------
// CONFIG
// ---------------------------
const DOWNLOADS_ROOT = path.resolve(process.env.DOWNLOADS_ROOT || "./downloads");

// Term mapping: GE year -> term number
// Source: List of parliaments of New Zealand (45th=1996 ... 54th=2023) :contentReference[oaicite:4]{index=4}
const YEAR_TO_TERM = {
  1996: 45,
  1999: 46,
  2002: 47,
  2005: 48,
  2008: 49,
  2011: 50,
  2014: 51,
  2017: 52,
  2020: 53,
  2023: 54,
};

// If you want to hard-route special by-election years to the “current term (GE year)”:
const BYELECTION_YEAR_TO_TERM_GE_YEAR = {
  // Example: 2025 by-election belongs under term_54_(2023)
  2025: 2023,
};

// ---------------------------
// UTIL
// ---------------------------
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function readJsonIfExists(p, fallback) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJson(p, obj) {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

function appendJsonl(p, obj) {
  ensureDir(path.dirname(p));
  fs.appendFileSync(p, JSON.stringify(obj) + "\n", "utf8");
}

function normalizeUrl(u) {
  if (!u) return "";
  // clean legacy ../../ etc
  try {
    const urlObj = new URL(u);
    // normalize path by resolving dot segments
    const parts = urlObj.pathname.split("/").filter(Boolean);
    const stack = [];
    for (const part of parts) {
      if (part === ".") continue;
      if (part === "..") stack.pop();
      else stack.push(part);
    }
    urlObj.pathname = "/" + stack.join("/");
    return urlObj.toString();
  } catch {
    return String(u);
  }
}

// Extract a “GE year” from /electionresults_YYYY/ URLs
function getGeYearFromUrl(u) {
  const m = String(u).match(/\/electionresults_(\d{4})\//);
  if (m) return Number(m[1]);
  return null;
}

// Extract by-election year from URL if present (heuristic)
function getByelectionYearFromUrl(u) {
  // e.g. ".../tamaki-makaurau-by-election-2025/..." or "...by-election...2025..."
  const m = String(u).match(/(?:by[- ]election|byelection)[^0-9]*(\d{4})/i);
  if (m) return Number(m[1]);
  // fallback: any trailing year segment
  const m2 = String(u).match(/(\d{4})(?:\/|$)/);
  if (m2) return Number(m2[1]);
  return null;
}

function termFolderNameFromGeYear(geYear) {
  const term = YEAR_TO_TERM[geYear];
  if (!term) return null;
  return `term_${term}_(${geYear})`;
}

function sanitizePathPart(s) {
  // keep macrons etc; just remove path separators and OS-invalid chars
  return sanitize(String(s)).replace(/\s+/g, "_");
}

function loadElectoratesByTerm() {
  const p = path.join(DOWNLOADS_ROOT, "_meta", "electorates_by_term.json");
  return readJsonIfExists(p, {}); // { "term_54_(2023)": { "Tāmaki Makaurau": 3, ... } }
}

function saveElectoratesByTerm(obj) {
  const p = path.join(DOWNLOADS_ROOT, "_meta", "electorates_by_term.json");
  writeJson(p, obj);
}

// Match electorate name in URL using mapping (best-effort)
function inferElectorateFromUrl(u, electorateMapForTerm) {
  if (!electorateMapForTerm) return null;

  const url = decodeURIComponent(String(u)).toLowerCase();

  // Build candidate match list sorted by length (longest first to avoid partial collisions)
  const names = Object.keys(electorateMapForTerm).sort((a, b) => b.length - a.length);

  for (const name of names) {
    const slug = name
      .toLowerCase()
      .replace(/[\u2019']/g, "")  // apostrophes
      .replace(/\s+/g, "-");

    // check either raw name or slug appears
    if (url.includes(name.toLowerCase()) || url.includes(slug)) {
      return name; // return canonical name
    }
  }

  return null;
}

function electorateFolderName(electorateName, electorateMapForTerm) {
  const n = electorateMapForTerm?.[electorateName];
  if (!n) return sanitizePathPart(electorateName);
  const num = String(n).padStart(3, "0");
  return `${num}_${sanitizePathPart(electorateName)}`;
}

// Decide where a file goes
function resolveOutputPath({ url, originalFilename }) {
  const cleanUrl = normalizeUrl(url);

  // 1) Determine GE year (term)
  let geYear = getGeYearFromUrl(cleanUrl);

  // 2) If not GE URL, try by-election heuristic (route to its parent term)
  if (!geYear) {
    const byYear = getByelectionYearFromUrl(cleanUrl);
    const parentGeYear = BYELECTION_YEAR_TO_TERM_GE_YEAR[byYear];
    if (parentGeYear) geYear = parentGeYear;
  }

  // 3) If still unknown, dump to _unresolved
  if (!geYear) {
    const termDir = path.join(DOWNLOADS_ROOT, "_unresolved");
    ensureDir(termDir);
    return {
      outputDir: termDir,
      outputFile: sanitizePathPart(originalFilename || "unknown.bin"),
      reason: "No GE year / term inferred",
      cleanUrl,
    };
  }

  const termFolder = termFolderNameFromGeYear(geYear);
  if (!termFolder) {
    const termDir = path.join(DOWNLOADS_ROOT, "_unresolved");
    ensureDir(termDir);
    return {
      outputDir: termDir,
      outputFile: sanitizePathPart(originalFilename || "unknown.bin"),
      reason: `No term mapping for geYear=${geYear}`,
      cleanUrl,
    };
  }

  const electoratesByTerm = loadElectoratesByTerm();
  const termKey = termFolder;
  const mapForTerm = electoratesByTerm[termKey];

  // 4) Electorate routing (if we can infer)
  const electorateName = inferElectorateFromUrl(cleanUrl, mapForTerm);
  let outDir = path.join(DOWNLOADS_ROOT, termFolder);

  if (electorateName) {
    outDir = path.join(outDir, electorateFolderName(electorateName, mapForTerm));
  }

  ensureDir(outDir);

  // filename: prefer original filename; else derive from URL pathname
  let filename = originalFilename;
  if (!filename) {
    try {
      const uo = new URL(cleanUrl);
      filename = path.basename(uo.pathname) || "download.bin";
    } catch {
      filename = "download.bin";
    }
  }

  return {
    outputDir: outDir,
    outputFile: sanitizePathPart(filename),
    reason: "ok",
    cleanUrl,
    termFolder,
    electorateName: electorateName || null,
  };
}

// ---------------------------
// UPLOAD HANDLING
// ---------------------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 200 * 1024 * 1024 }, // 200MB per file
});

// POST /sink (multipart/form-data):
// fields: url (string), filename (string optional)
// file: "file" (binary)
app.post("/sink", upload.single("file"), (req, res) => {
  try {
    const url = req.body?.url || req.query?.url;
    const filename = req.body?.filename || req.query?.filename;

    if (!url) {
      return res.status(400).json({ ok: false, error: "Missing url field" });
    }
    if (!req.file?.buffer) {
      return res.status(400).json({ ok: false, error: "Missing file content (multipart 'file')" });
    }

    const routing = resolveOutputPath({ url, originalFilename: filename || req.file.originalname });
    const outPath = path.join(routing.outputDir, routing.outputFile);

    // Overwrite by default
    fs.writeFileSync(outPath, req.file.buffer);

    res.json({
      ok: true,
      saved_to: outPath,
      term: routing.termFolder || null,
      electorate: routing.electorateName,
      url: routing.cleanUrl,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// Optional: JSON base64 upload (if you ever want it)
// POST /sink/base64 { url, filename, content_base64 }
app.post("/sink/base64", (req, res) => {
  try {
    const { url, filename, content_base64 } = req.body || {};
    if (!url || !content_base64) {
      return res.status(400).json({ ok: false, error: "Need {url, content_base64}" });
    }
    const buf = Buffer.from(content_base64, "base64");
    const routing = resolveOutputPath({ url, originalFilename: filename });
    const outPath = path.join(routing.outputDir, routing.outputFile);
    fs.writeFileSync(outPath, buf);
    res.json({ ok: true, saved_to: outPath, term: routing.termFolder || null, electorate: routing.electorateName });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// ---------------------------
// ELECTORATE MAP INGEST
// ---------------------------
// POST /meta/electorates
// {
//   "geYear": 2023,
//   "electorates": ["Auckland Central", "Banks Peninsula", ...]  // in OFFICIAL ORDER for that term
// }
app.post("/meta/electorates", (req, res) => {
  try {
    const { geYear, electorates } = req.body || {};
    if (!geYear || !Array.isArray(electorates) || electorates.length === 0) {
      return res.status(400).json({ ok: false, error: "Need {geYear:number, electorates:string[]}" });
    }
    const termFolder = termFolderNameFromGeYear(Number(geYear));
    if (!termFolder) {
      return res.status(400).json({ ok: false, error: `Unknown geYear ${geYear}` });
    }

    const electoratesByTerm = loadElectoratesByTerm();
    electoratesByTerm[termFolder] = electorates.reduce((acc, name, idx) => {
      acc[String(name)] = idx + 1; // 1-based official order
      return acc;
    }, {});

    saveElectoratesByTerm(electoratesByTerm);

    res.json({ ok: true, termFolder, count: electorates.length });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// Capture unresolved URLs if you want a quick audit trail
app.post("/meta/unresolved", (req, res) => {
  try {
    appendJsonl(path.join(DOWNLOADS_ROOT, "_meta", "unresolved_urls.jsonl"), {
      ts: new Date().toISOString(),
      ...req.body,
    });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get("/health", (req, res) => res.json({ ok: true }));

// ---------------------------
// START
// ---------------------------
ensureDir(DOWNLOADS_ROOT);
ensureDir(path.join(DOWNLOADS_ROOT, "_meta"));

const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`sink listening on http://localhost:${port}`);
  console.log(`downloads root: ${DOWNLOADS_ROOT}`);
});
