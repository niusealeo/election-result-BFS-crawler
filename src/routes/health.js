const express = require("express");
const { cfgForReq } = require("../lib/domain");

function makeHealthRouter(baseCfg) {
  const r = express.Router();
  r.get("/health", (req, res) => {
    const cfg = cfgForReq(baseCfg, req);
    res.json({
      ok: true,
      domain_key: cfg.domain_key,
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
