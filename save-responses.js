// save-responses.js
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const SCHEMA = ["term","poll_type","electorate","dataset","start","end","step","url_template","outfile"];

function ensureDir(p){ fs.mkdirSync(p, { recursive: true }); }

function tsTag(){
  const d = new Date();
  const pad = n => String(n).padStart(2,"0");
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function sanitizePart(s){
  return String(s ?? "")
    .trim()
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, "_") // strips path separators too
    .replace(/\s+/g, " ")
    .slice(0, 180);
}

function slug(s){
  return sanitizePart(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "unknown";
}

function shortHash(s){
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 10);
}

function csvEscape(v){
  const s = String(v ?? "");
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
}

// Optional: protect against Excel formula injection on logs
function excelSafe(s){
  const v = String(s ?? "");
  return /^[=+\-@]/.test(v) ? `'${v}` : v;
}

function isCloudflareBlockHtml(buf){
  const head = buf.slice(0, 6000).toString("utf8").toLowerCase();
  return head.includes("attention required") && head.includes("cloudflare") &&
    (head.includes("you have been blocked") || head.includes("sorry, you have been blocked") || head.includes("unable to access"));
}

function extFromContentType(ct){
  if (!ct) return null;
  ct = ct.toLowerCase();
  if (ct.includes("text/csv")) return ".csv";
  if (ct.includes("application/pdf")) return ".pdf";
  if (ct.includes("application/json")) return ".json";
  if (ct.includes("text/plain")) return ".txt";
  if (ct.includes("text/html")) return ".html";
  if (ct.includes("application/zip")) return ".zip";
  return null;
}

function filenameFromHeadersOrUrl(res, url){
  const cd = res.headers["content-disposition"];
  const ct = res.headers["content-type"];

  // content-disposition filename
  if (cd && /filename\s*=/.test(cd.toLowerCase())) {
    const m = cd.match(/filename\*?=(?:UTF-8''|")?([^";\r\n]+)"?/i);
    if (m && m[1]) {
      try { return sanitizePart(decodeURIComponent(m[1])) || null; }
      catch { return sanitizePart(m[1]) || null; }
    }
  }

  // URL basename
  try {
    const u = new URL(url);
    let base = decodeURIComponent(u.pathname.split("/").pop() || "");
    base = sanitizePart(base);
    if (base && base.includes(".")) return base;
  } catch {}

  // content-type fallback
  const ext = extFromContentType(ct);
  if (ext) return `file${ext}`;
  return "file.bin";
}

function buildRelDir(row){
  const termDir = row.term ? `term_${slug(row.term)}` : "term_unknown";
  const pt = slug(row.poll_type || "");

  if (pt === "general_election") {
    const ds = row.dataset ? slug(row.dataset) : "misc";
    return path.join(termDir, "general_election", ds);
  }
  if (pt === "by_elections") return path.join(termDir, "by_elections");
  if (pt === "referendums") return path.join(termDir, "referendums");

  return path.join(termDir, pt || "unknown_poll_type");
}

function protocolOk(u){
  try {
    const p = new URL(u).protocol;
    return p === "http:" || p === "https:";
  } catch { return false; }
}

module.exports = function (emitter, reporterOptions) {
  const outBase = reporterOptions.export || "downloads";
  const runTag = reporterOptions.tag || tsTag();
  const failWritten = new Map(); // failPath -> true

  function failPathFor(sourceFile){
    const base = path.basename(sourceFile || "unknown.csv", ".csv");
    return path.join(outBase, "_failed", `${base}__newman__${runTag}.csv`);
  }

  function writeFailure(row, sourceFile, status, reason){
    const fp = failPathFor(sourceFile);
    ensureDir(path.dirname(fp));
    if (!failWritten.has(fp)) {
      fs.writeFileSync(fp, ["source_file", ...SCHEMA, "status", "reason"].join(",") + "\n", "utf8");
      failWritten.set(fp, true);
    }
    const line = [
      excelSafe(sourceFile),
      ...SCHEMA.map(k => excelSafe(row[k] ?? "")),
      status,
      reason
    ].map(csvEscape).join(",") + "\n";
    fs.appendFileSync(fp, line, "utf8");
  }

  emitter.on("request", function (err, args) {
    const row = args?.cursor?.iterationData?.toObject?.() || {};
    const sourceFile = args?.cursor?.variables?.get?.("source_file") || "unknown.csv";
    const reqUrl = row.url_template || "";

    if (!protocolOk(reqUrl)) {
      writeFailure(row, sourceFile, 0, "bad_url_protocol");
      console.log(`[FAILED] bad url protocol: ${reqUrl}`);
      return;
    }

    const res = args?.response;
    if (err || !res) {
      writeFailure(row, sourceFile, 0, `request_error`);
      console.log(`[FAILED] request error: ${reqUrl}`);
      return;
    }

    const status = res.code ?? 0;
    const body = res.stream || Buffer.from("");

    if (status >= 400) {
      writeFailure(row, sourceFile, status, `http_${status}`);
      console.log(`[FAILED] ${status} ${reqUrl}`);
      return;
    }

    if (isCloudflareBlockHtml(body)) {
      writeFailure(row, sourceFile, status, "cloudflare_block_page");
      console.log(`[FAILED] CF_BLOCK ${reqUrl}`);
      return;
    }

    // build target dir
    const relDir = buildRelDir(row);

    // outfile sanitization (filename only)
    const explicitOut = (row.outfile || "").trim();
    let fname = explicitOut ? sanitizePart(explicitOut) : filenameFromHeadersOrUrl(res, reqUrl);
    fname = sanitizePart(fname) || "file.bin";

    // If explicit outfile has no extension but content-type suggests one, add it
    if (explicitOut && !path.extname(fname)) {
      const ext = extFromContentType(res.headers["content-type"]);
      if (ext) fname += ext;
    }

    let fullPath = path.join(outBase, relDir, fname);

    // avoid collisions
    if (fs.existsSync(fullPath)) {
      const ext = path.extname(fullPath);
      const base = fullPath.slice(0, fullPath.length - ext.length);
      fullPath = `${base}__${shortHash(reqUrl)}${ext || ""}`;
    }

    ensureDir(path.dirname(fullPath));
    fs.writeFileSync(fullPath, body);

    console.log(`[saved] ${fullPath} (${body.length} bytes)`);
  });
};
