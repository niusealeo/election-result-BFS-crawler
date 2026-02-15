const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const SCHEMA = ["term","poll_type","electorate","dataset","start","end","step","url_template","outfile"];

function ensureDir(p){ fs.mkdirSync(p, { recursive:true }); }

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
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0,8);
}

function tsTag(){
  const d = new Date();
  const pad = n => String(n).padStart(2,"0");
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function csvEscape(v){
  const s = String(v ?? "");
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
}

function buildRelDir(row){
  const term = `term_${slug(row.term)}`;
  const pt = slug(row.poll_type);

  if (pt === "general_election")
    return path.join(term, "general_election", slug(row.dataset));

  if (pt === "by_elections")
    return path.join(term, "by_elections");

  if (pt === "referendums")
    return path.join(term, "referendums");

  return path.join(term, pt);
}

module.exports = function (emitter, reporterOptions) {

  const baseDir = reporterOptions.export || "downloads";
  const runTag = reporterOptions.tag || tsTag();
  const failDir = path.join(baseDir, "_failed");

  emitter.on("request", function (err, args) {

    const row = args.cursor.iterationData?.toObject?.() || {};
    const sourceFile = args.cursor.variables?.get("source_file") || "unknown.csv";
    const res = args.response;
    const url = row.url_template;

    if (!res || res.code >= 400) {
      ensureDir(failDir);
      const failPath = path.join(failDir, `${path.basename(sourceFile,".csv")}__newman__${runTag}.csv`);
      if (!fs.existsSync(failPath))
        fs.writeFileSync(failPath, ["source_file",...SCHEMA,"status","reason"].join(",")+"\n");

      const line = [
        sourceFile,
        ...SCHEMA.map(k=>row[k] ?? ""),
        res?.code ?? 0,
        "http_error"
      ].map(csvEscape).join(",")+"\n";

      fs.appendFileSync(failPath,line);
      return;
    }

    const body = res.stream;
    const relDir = buildRelDir(row);
    ensureDir(path.join(baseDir, relDir));

    let fname = row.outfile?.trim()
      ? sanitizePart(row.outfile)
      : sanitizePart(new URL(url).pathname.split("/").pop());

    if (!fname) fname = `file_${shortHash(url)}.bin`;

    const fullPath = path.join(baseDir, relDir, fname);

    fs.writeFileSync(fullPath, body);
    console.log("[saved]", fullPath);
  });
};
