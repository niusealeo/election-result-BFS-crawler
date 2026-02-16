const fs = require("fs");
const path = require("path");
const { ensureDir } = require("./fsx");

function appendJsonl(p, obj) {
  ensureDir(path.dirname(p));
  fs.appendFileSync(p, JSON.stringify(obj) + "\n", "utf8");
}

module.exports = { appendJsonl };
