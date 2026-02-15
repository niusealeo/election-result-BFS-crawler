// discover_to_knownfiles.js
const fs = require("fs");
const path = require("path");
const newman = require("newman");

const SCHEMA = ["term","poll_type","electorate","dataset","start","end","step","url_template","outfile"];

function tsTag(){
  const d = new Date();
  const pad = n => String(n).padStart(2,"0");
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function csvEscape(v){
  const s = String(v ?? "");
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
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

function protocolOk(u){
  try {
    const p = new URL(u).protocol;
    return p === "http:" || p === "https:";
  } catch { return false; }
}

function isCloudflareBlockHtml(buf){
  const head = buf.slice(0, 6000).toString("utf8").toLowerCase();
  return head.includes("attention required") && head.includes("cloudflare") &&
    (head.includes("you have been blocked") || head.includes("sorry, you have been blocked") || head.includes("unable to access"));
}

// very simple href extractor (no deps)
function extractHrefs(html){
  const out = [];
  const re = /href\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s>]+))/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const href = (m[1] || m[2] || m[3] || "").trim();
    if (!href) continue;
    if (href.startsWith("#") || href.toLowerCase().startsWith("javascript:") || href.toLowerCase().startsWith("mailto:")) continue;
    out.push(href);
  }
  return out;
}

function absUrl(base, href){
  try { return new URL(href, base).toString(); } catch { return null; }
}

function writeSchemaCsv(fp, rows){
  const header = SCHEMA.join(",");
  const lines = [header];
  for (const r of rows) {
    lines.push(SCHEMA.map(k => csvEscape(r[k] ?? "")).join(","));
  }
  fs.writeFileSync(fp, lines.join("\n") + "\n", "utf8");
}

// MAIN
const inputCsv = process.argv[2];
if (!inputCsv) {
  console.error("Usage: node discover_to_knownfiles.js <directories_schema.csv>");
  process.exit(1);
}

const tag = tsTag();
const outDir = "discovery_out";
fs.mkdirSync(outDir, { recursive: true });

const outCsv = path.join(outDir, `discovered__${path.basename(inputCsv, ".csv")}__${tag}.csv`);
const failCsv = path.join(outDir, `discovery_failed__${path.basename(inputCsv, ".csv")}__${tag}.csv`);

const inputRowsRaw = readCsvSimple(inputCsv);

// discovery input should be direct pages; ignore ranges here
const inputRows = inputRowsRaw
  .map(r => {
    const rr = {};
    for (const k of SCHEMA) rr[k] = r[k] ?? "";
    return rr;
  })
  .filter(r => !r.start && !r.end && r.url_template && protocolOk(r.url_template));

const discovered = [];
const failed = [];

const seen = new Set();

newman.run({
  collection: require(path.resolve("collection.json")),
  iterationData: inputRows,
  envVar: [{ key: "source_file", value: path.basename(inputCsv) }],
  delayRequest: 250,
  reporters: ["cli"]
})
.on("request", function (err, args) {
  const row = args?.cursor?.iterationData?.toObject?.() || {};
  const url = row.url_template || "";
  const res = args?.response;

  if (err || !res) {
    failed.push({ ...row, reason: "request_error" });
    return;
  }
  const status = res.code ?? 0;
  const body = res.stream || Buffer.from("");

  if (status >= 400) {
    failed.push({ ...row, reason: `http_${status}` });
    return;
  }
  if (isCloudflareBlockHtml(body)) {
    failed.push({ ...row, reason: "cloudflare_block_page" });
    return;
  }

  const ct = (res.headers["content-type"] || "").toLowerCase();
  if (!ct.includes("text/html")) {
    // If the “directory page” is actually a file, just keep it as a discovered direct URL
    const key = url;
    if (!seen.has(key)) {
      seen.add(key);
      const outRow = { ...row };
      outRow.start = ""; outRow.end = ""; outRow.step = "";
      outRow.outfile = outRow.outfile || "";
      discovered.push(outRow);
    }
    return;
  }

  const html = body.toString("utf8");
  const hrefs = extractHrefs(html);

  for (const h of hrefs) {
    const u = absUrl(url, h);
    if (!u || !protocolOk(u)) continue;

    const key = u;
    if (seen.has(key)) continue;
    seen.add(key);

    // Emit as direct known-files row: same metadata, url_template = discovered url
    const outRow = { ...row };
    outRow.start = ""; outRow.end = ""; outRow.step = "";
    outRow.url_template = u;
    outRow.outfile = ""; // keep blank; downloader decides
    discovered.push(outRow);
  }
})
.on("done", function () {
  writeSchemaCsv(outCsv, discovered);

  if (failed.length) {
    // discovery fail csv is also schema + (we put reason into outfile to keep schema-only)
    // You can choose a separate log format; keeping schema-only was your constraint.
    const failedSchema = failed.map(r => {
      const rr = { ...r };
      rr.outfile = rr.outfile ? rr.outfile : `DISCOVERY_FAIL:${r.reason || "unknown"}`;
      return rr;
    });
    writeSchemaCsv(failCsv, failedSchema);
  }

  console.log(`\n[discovery] discovered=${discovered.length}`);
  console.log(`[discovery] out=${outCsv}`);
  if (failed.length) console.log(`[discovery] failed=${failCsv}`);
});
