const express = require("express");
const cfg = require("./config");
const { ensureDir } = require("./lib/fsx");

const { makeHealthRouter } = require("./routes/health");
const { makeDedupeRouter } = require("./routes/dedupe");
const { makeUploadRouter } = require("./routes/upload");
const { makeElectoratesRouter } = require("./routes/electorates");
const { makeRunsRouter } = require("./routes/runs");

ensureDir(cfg.BFS_ROOT);
ensureDir(cfg.RUNS_DIR);
ensureDir(cfg.META_DIR);
ensureDir(cfg.ARTIFACT_DIR);
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
});
