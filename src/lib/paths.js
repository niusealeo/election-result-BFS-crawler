const path = require("path");

// Project root is the current working directory when running sink.
function projectRoot() {
  return process.cwd();
}

function toAbsolute(relPath) {
  if (!relPath) return null;
  return path.resolve(projectRoot(), relPath);
}

function toRelative(absPath) {
  if (!absPath) return null;
  return path.relative(projectRoot(), absPath);
}

module.exports = {
  projectRoot,
  toAbsolute,
  toRelative,
};
