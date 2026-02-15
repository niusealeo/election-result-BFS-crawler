const fs = require("fs");
const path = require("path");
const newman = require("newman");

const SCHEMA = ["term","poll_type","electorate","dataset","start","end","step","url_template","outfile"];

function readCsvSimple(fp){
  const txt = fs.readFileSync(fp,"utf8").trim();
  const lines = txt.split(/\r?\n/);
  const header = lines.shift().split(",");
  return lines.filter(Boolean).map(line=>{
    const parts=line.split(",");
    const row={};
    header.forEach((h,i)=>row[h]=parts[i] ?? "");
    return row;
  });
}

function expandRows(rows){
  const out=[];
  for(const r of rows){
    if(r.start && r.end){
      const start=parseInt(r.start,10);
      const end=parseInt(r.end,10);
      const step=r.step?parseInt(r.step,10):1;
      for(let i=start;i<=end;i+=step){
        const rr={...r};
        rr.url_template=String(r.url_template).replaceAll("{{i}}",i);
        rr.outfile=String(r.outfile||"").replaceAll("{{i}}",i);
        out.push(rr);
      }
    } else {
      out.push(r);
    }
  }
  return out;
}

const inputCsv = process.argv[2];
if(!inputCsv){
  console.error("Usage: node run_newman_expanded.js <csv>");
  process.exit(1);
}

const rows = expandRows(readCsvSimple(inputCsv));

newman.run({
  collection: require(path.resolve("collection.json")),
  iterationData: rows,
  envVar: [{ key:"source_file", value:path.basename(inputCsv) }],
  reporters:["cli", path.resolve("./save-responses.js")],
  reporter:{
    [path.resolve("./save-responses.js")]: { export:"downloads" }
  },
  delayRequest:250
}, err=>{
  if(err) process.exit(2);
});
