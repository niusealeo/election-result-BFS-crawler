const express = require("express");
const cfg = require("./config");
const { ensureDir } = require("./lib/fsx");

const { makeHealthRouter } = require("./routes/health");
const { makeDedupeRouter } = require("./routes/dedupe");
const { makeUploadRouter } = require("./routes/upload");
const { makeElectoratesRouter } = require("./routes/electorates");
const { makeRunsRouter } = require("./routes/runs");

// Ensure folders exist (same as old sink.js)
ensureDir(cfg.BFS_ROOT);
ensureDir(cfg.RUNS_DIR);
ensureDir(cfg.META_DIR);
ensureDir(cfg.ARTIFACT_DIR);
ensureDir(cfg.LEVEL_FILES_DIR);
ensureDir(cfg.DOWNLOADS_ROOT);

const app = express();
app.use(express.json({ limit: "750mb" }));

app.use(makeHealthRouter(cfg));
app.use(makeDedupeRouter(cfg));
app.use(makeUploadRouter(cfg));
app.use(makeElectoratesRouter(cfg));
app.use(makeRunsRouter(cfg));

app.listen(cfg.PORT, () => {
  console.log(`sink listening on http://localhost:${cfg.PORT}`);
  console.log(`BFS_ROOT: ${cfg.BFS_ROOT}`);
  console.log(`DOWNLOADS_ROOT: ${cfg.DOWNLOADS_ROOT}`);
  console.log(`META_DIR: ${cfg.META_DIR}`);
  console.log(`RUNS_DIR: ${cfg.RUNS_DIR}`);
  console.log(`ARTIFACT_META_FIRST_ROW: ${cfg.ARTIFACT_META_FIRST_ROW}`);
});
