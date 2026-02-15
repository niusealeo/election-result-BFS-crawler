// download_curl.js
const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");
const crypto = require("crypto");

const SCHEMA = ["term","poll_type","electorate","dataset","start","end","step","url_template","outfile"];

function ensureDir(p){ fs.mkdirSync(p, { recursive:true }); }

function tsTag(){
  const d = new Date();
  const pad = n => String(n).padStart(2,"0");
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function sanitizePart(s){
  return String(s ?? "")
    .trim()
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, "_")
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

// Optional: protect Excel logs
function excelSafe(s){
  const v = String(s ?? "");
  return /^[=+\-@]/.test(v) ? `'${v}` : v;
}

function readCsvSimple(fp){
  const txt = fs.readFileSync(fp, "utf8").trim();
  const lines = txt.split(/\r?\n/);
  const header = lines.shift().split(",");
  return lines.filter(l => l.trim()).map(line => {
    const parts = line.split(",");
    const row = {};
    header.forEach((h,i)=> row[h] = (parts[i] ?? ""));
    return row;
  });
}

function expandRows(rows){
  const out = [];
  for (const r of rows) {
    if (r.start && r.end) {
      const start = parseInt(r.start, 10);
      const end = parseInt(r.end, 10);
      const step = r.step ? parseInt(r.step, 10) : 1;

      for (let i = start; i <= end; i += step) {
        const rr = {};
        for (const k of SCHEMA) rr[k] = r[k] ?? "";
        rr.url_template = String(rr.url_template || "").replaceAll("{{i}}", String(i));
        rr.outfile = String(rr.outfile || "").replaceAll("{{i}}", String(i));
        out.push(rr);
      }
    } else {
      const rr = {};
      for (const k of SCHEMA) rr[k] = r[k] ?? "";
      out.push(rr);
    }
  }
  return out;
}

function protocolOk(u){
  try {
    const p = new URL(u).protocol;
    return p === "http:" || p === "https:";
  } catch { return false; }
}

function isCloudflareBlockHtmlFile(fp){
  try {
    const buf = fs.readFileSync(fp);
    const head = buf.slice(0, 6000).toString("utf8").toLowerCase();
    return head.includes("attention required") && head.includes("cloudflare") &&
      (head.includes("you have been blocked") || head.includes("sorry, you have been blocked") || head.includes("unable to access"));
  } catch { return false; }
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

function filenameFromUrl(url){
  try {
    const u = new URL(url);
    let base = decodeURIComponent(u.pathname.split("/").pop() || "");
    base = sanitizePart(base);
    if (!base) base = "file.bin";
    if (!base.includes(".")) base += ".bin";
    return base;
  } catch {
    return "file.bin";
  }
}

function writeFailHeaderIfNeeded(fp){
  if (!fs.existsSync(fp)) {
    ensureDir(path.dirname(fp));
    fs.writeFileSync(fp, ["source_file", ...SCHEMA, "status", "reason"].join(",") + "\n", "utf8");
  }
}

function appendFail(fp, sourceFile, row, status, reason){
  writeFailHeaderIfNeeded(fp);
  const line = [
    excelSafe(sourceFile),
    ...SCHEMA.map(k => excelSafe(row[k] ?? "")),
    status,
    reason
  ].map(csvEscape).join(",") + "\n";
  fs.appendFileSync(fp, line, "utf8");
}

// MAIN
const inputCsv = process.argv[2];
if (!inputCsv) {
  console.error("Usage: node download_curl.js <known_files.csv>");
  process.exit(1);
}

const sourceFile = path.basename(inputCsv);
const runTag = tsTag();
const failCsv = path.join("downloads", "_failed", `${path.basename(sourceFile, ".csv")}__curl__${runTag}.csv`);

const rows = expandRows(readCsvSimple(inputCsv));

let ok = 0, fail = 0;

for (const row of rows) {
  const url = row.url_template || "";
  if (!protocolOk(url)) {
    fail++;
    appendFail(failCsv, sourceFile, row, 0, "bad_url_protocol");
    continue;
  }

  const relDir = buildRelDir(row);
  const baseDir = path.join("downloads", relDir);

  // outfile is filename-only; sanitize strips slashes
  const explicitOut = (row.outfile || "").trim();
  let fname = explicitOut ? sanitizePart(explicitOut) : filenameFromUrl(url);
  fname = sanitizePart(fname) || `file__${shortHash(url)}.bin`;

  // avoid collisions
  let outPath = path.join(baseDir, fname);
  ensureDir(path.dirname(outPath));
  if (fs.existsSync(outPath)) {
    const ext = path.extname(outPath);
    const base = outPath.slice(0, outPath.length - ext.length);
    outPath = `${base}__${shortHash(url)}${ext || ""}`;
  }

  // injection hardening:
  // - no shell
  // - use "--" before URL to prevent curl option injection
  const curl = spawnSync("curl", [
    "-L", "--fail",
    "--silent", "--show-error",
    "--retry", "3", "--retry-delay", "1",
    "-o", outPath,
    "--", url
  ], { stdio: "inherit" });

  if ((curl.status ?? 1) !== 0) {
    fail++;
    appendFail(failCsv, sourceFile, row, curl.status ?? 1, "curl_failed");
    try { if (fs.existsSync(outPath)) fs.unlinkSync(outPath); } catch {}
    continue;
  }

  if (isCloudflareBlockHtmlFile(outPath)) {
    fail++;
    appendFail(failCsv, sourceFile, row, 200, "cloudflare_html");
    try { if (fs.existsSync(outPath)) fs.unlinkSync(outPath); } catch {}
    continue;
  }

  ok++;
}

console.log(`\n[curl] ok=${ok} fail=${fail}`);

if (fail > 0) {
  console.log(`[curl fail csv] ${failCsv}`);
  console.log("[newman] auto-fallback starting...");
  const r = spawnSync("node", ["run_newman_expanded.js", failCsv], { stdio: "inherit" });
  process.exit(r.status ?? 1);
}

process.exit(0);
