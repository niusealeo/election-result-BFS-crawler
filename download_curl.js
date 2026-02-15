const fs=require("fs");
const path=require("path");
const {spawnSync}=require("child_process");

const SCHEMA=["term","poll_type","electorate","dataset","start","end","step","url_template","outfile"];

function ensureDir(p){fs.mkdirSync(p,{recursive:true});}
function ts(){const d=new Date();const p=n=>String(n).padStart(2,"0");return `${d.getFullYear()}${p(d.getMonth()+1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;}
function slug(s){return String(s??"").toLowerCase().replace(/[^a-z0-9]+/g,"_").replace(/^_+|_+$/g,"")||"unknown";}
function readCsv(fp){const t=fs.readFileSync(fp,"utf8").trim().split(/\r?\n/);const h=t.shift().split(",");return t.filter(Boolean).map(l=>{const p=l.split(",");const r={};h.forEach((k,i)=>r[k]=p[i]??"");return r;});}

function buildDir(r){
  const term=`term_${slug(r.term)}`;
  if(r.poll_type==="general_election")
    return path.join(term,"general_election",slug(r.dataset));
  if(r.poll_type==="by_elections")
    return path.join(term,"by_elections");
  if(r.poll_type==="referendums")
    return path.join(term,"referendums");
  return path.join(term,slug(r.poll_type));
}

function expand(rows){
  const out=[];
  for(const r of rows){
    if(r.start&&r.end){
      const start=parseInt(r.start,10);
      const end=parseInt(r.end,10);
      const step=r.step?parseInt(r.step,10):1;
      for(let i=start;i<=end;i+=step){
        const rr={...r};
        rr.url_template=String(r.url_template).replaceAll("{{i}}",i);
        rr.outfile=String(r.outfile||"").replaceAll("{{i}}",i);
        out.push(rr);
      }
    } else out.push(r);
  }
  return out;
}

function validUrl(u){
  try{
    const p=new URL(u).protocol;
    return p==="http:"||p==="https:";
  }catch{return false;}
}

const input=process.argv[2];
if(!input){console.error("Usage: node download_curl.js <csv>");process.exit(1);}

const rows=expand(readCsv(input));
const failPath=path.join("downloads","_failed",`${path.basename(input,".csv")}__curl__${ts()}.csv`);

let fail=0;

for(const r of rows){
  const url=r.url_template;
  if(!validUrl(url)){fail++;continue;}

  const dir=path.join("downloads",buildDir(r));
  ensureDir(dir);

  const fname=r.outfile?.trim()||new URL(url).pathname.split("/").pop()||"file.bin";
  const outPath=path.join(dir,fname);

  const curl=spawnSync("curl",[
    "-L","--fail","--silent","--show-error",
    "--retry","3","--retry-delay","1",
    "-o",outPath,
    "--",url
  ],{stdio:"inherit"});

  if((curl.status??1)!==0){
    fail++;
    ensureDir(path.dirname(failPath));
    if(!fs.existsSync(failPath))
      fs.writeFileSync(failPath,["source_file",...SCHEMA,"status","reason"].join(",")+"\n");
    const line=[
      path.basename(input),
      ...SCHEMA.map(k=>r[k]??""),
      curl.status??1,
      "curl_failed"
    ].join(",")+"\n";
    fs.appendFileSync(failPath,line);
  }
}

if(fail>0){
  console.log("[curl fallback to newman]");
  spawnSync("node",["run_newman_expanded.js",failPath],{stdio:"inherit"});
}
