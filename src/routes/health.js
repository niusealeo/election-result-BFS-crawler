const express = require("express");

function makeHealthRouter(cfg) {
  const r = express.Router();
  r.get("/health", (_req, res) => {
    res.json({
      ok: true,
      BFS_ROOT: cfg.BFS_ROOT,
      DOWNLOADS_ROOT: cfg.DOWNLOADS_ROOT,
      META_DIR: cfg.META_DIR,
      RUNS_DIR: cfg.RUNS_DIR,
      ARTIFACT_META_FIRST_ROW: cfg.ARTIFACT_META_FIRST_ROW,
    });
  });
  return r;
}

module.exports = { makeHealthRouter };
