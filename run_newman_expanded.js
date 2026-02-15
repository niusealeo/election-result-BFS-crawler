// run_newman_expanded.js
const fs = require("fs");
const path = require("path");
const newman = require("newman");

const SCHEMA = ["term","poll_type","electorate","dataset","start","end","step","url_template","outfile"];

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
      const end   = parseInt(r.end, 10);
      const step  = r.step ? parseInt(r.step, 10) : 1;

      for (let i = start; i <= end; i += step) {
        const rr = {};
        for (const k of SCHEMA) rr[k] = r[k] ?? "";
        rr.url_template = String(rr.url_template || "").replaceAll("{{i}}", String(i));
        rr.outfile = String(rr.outfile || "").replaceAll("{{i}}", String(i));
        // keep start/end/step as original row values (schema unchanged)
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

const inputCsv = process.argv[2];
if (!inputCsv) {
  console.error("Usage: node run_newman_expanded.js <known_files.csv>");
  process.exit(1);
}

const rows = expandRows(readCsvSimple(inputCsv));
const sourceFile = path.basename(inputCsv);

newman.run({
  collection: require(path.resolve("collection.json")),
  iterationData: rows,
  envVar: [{ key: "source_file", value: sourceFile }],
  delayRequest: 250,
  reporters: ["cli", path.resolve("./save-responses.js")],
  reporter: {
    [path.resolve("./save-responses.js")]: { export: "downloads" }
  }
}, function (err) {
  if (err) process.exit(2);
});
