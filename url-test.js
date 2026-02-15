// compare_clients.js
// Run with: node compare_clients.js

const { spawnSync } = require("child_process");
const newman = require("newman");
const fs = require("fs");
const path = require("path");

// 🔴 CHANGE THIS URL TO TEST
const TEST_URL = "https://electionresults.govt.nz/electionresults_2023/statistics/csv/candidate-votes-by-voting-place-1.csv";

function printDivider(title) {
  console.log("\n====================================================");
  console.log(title);
  console.log("====================================================");
}

function printOutput(result) {
  if (result.error) {
    console.log("ERROR:", result.error.message);
    return;
  }

  if (result.status !== undefined) {
    console.log("Status:", result.status);
  }

  if (result.headers) {
    console.log("Headers:");
    for (const [k, v] of Object.entries(result.headers)) {
      console.log(`  ${k}: ${v}`);
    }
  }

  if (result.body) {
    console.log("\nBody (first 400 chars):");
    console.log(result.body.slice(0, 400));
  }
}

/* ===============================
   CURL TEST
================================= */

function testCurl() {
  printDivider("CURL");

  const res = spawnSync("curl", [
    "-L",
    "-s",
    "-D", "-",     // dump headers to stdout
    "--max-time", "15",
    "--",
    TEST_URL
  ], { encoding: "utf8" });

  if (res.error) {
    printOutput({ error: res.error });
    return;
  }

  const raw = res.stdout || "";
  const parts = raw.split("\r\n\r\n");
  const headerBlock = parts[0] || "";
  const body = parts.slice(1).join("\r\n\r\n");

  const headers = {};
  headerBlock.split("\r\n").slice(1).forEach(line => {
    const idx = line.indexOf(":");
    if (idx > -1) {
      headers[line.slice(0, idx).trim()] = line.slice(idx + 1).trim();
    }
  });

  const statusMatch = headerBlock.match(/HTTP\/[0-9.]+\s+(\d+)/);
  const status = statusMatch ? Number(statusMatch[1]) : null;

  printOutput({ status, headers, body });
}

/* ===============================
   WGET TEST
================================= */

function testWget() {
  printDivider("WGET");

  const res = spawnSync("wget", [
    "-q",
    "--server-response",
    "--output-document=-",
    TEST_URL
  ], { encoding: "utf8" });

  if (res.error) {
    printOutput({ error: res.error });
    return;
  }

  // wget writes headers to stderr
  const headerBlock = res.stderr || "";
  const body = res.stdout || "";

  const headers = {};
  let status = null;

  headerBlock.split("\n").forEach(line => {
    line = line.trim();
    if (line.startsWith("HTTP/")) {
      const m = line.match(/HTTP\/[0-9.]+\s+(\d+)/);
      if (m) status = Number(m[1]);
    } else if (line.includes(":")) {
      const idx = line.indexOf(":");
      headers[line.slice(0, idx).trim()] = line.slice(idx + 1).trim();
    }
  });

  printOutput({ status, headers, body });
}

/* ===============================
   NEWMAN TEST
================================= */

function testNewman(callback) {
  printDivider("NEWMAN");

  const collection = {
    info: {
      name: "Single Test",
      schema: "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
    },
    item: [{
      name: "Fetch",
      request: {
        method: "GET",
        url: TEST_URL
      }
    }]
  };

  newman.run({
    collection,
    reporters: []
  }, function (err, summary) {
    if (err) {
      printOutput({ error: err });
      callback();
      return;
    }

    const exec = summary.run.executions[0];
    const res = exec.response;

    const headers = {};
    res.headers.members.forEach(h => {
      headers[h.key] = h.value;
    });

    printOutput({
      status: res.code,
      headers,
      body: res.stream.toString("utf8")
    });

    callback();
  });
}

/* ===============================
   POSTMAN CLI TEST
================================= */

function testPostmanCLI() {
  printDivider("POSTMAN CLI");

  const tempCollection = {
    info: {
      name: "CLI Test",
      schema: "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
    },
    item: [{
      name: "Fetch",
      request: {
        method: "GET",
        url: TEST_URL
      }
    }]
  };

  const tempPath = path.join(__dirname, "temp_collection.json");
  fs.writeFileSync(tempPath, JSON.stringify(tempCollection, null, 2));

  const res = spawnSync("npx", [
    "postman",
    "collection",
    "run",
    tempPath,
    "--reporters", "cli"
  ], { encoding: "utf8" });

  if (res.error) {
    printOutput({ error: res.error });
    return;
  }

  console.log(res.stdout.slice(0, 800));
}

/* ===============================
   RUN ALL
================================= */

async function runAll() {
  testCurl();
  testWget();

  testNewman(() => {
    testPostmanCLI();
  });
}

runAll();
