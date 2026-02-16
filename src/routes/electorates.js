const express = require("express");
const { appendJsonl } = require("../lib/jsonl");
const { cleanElectorateName, loadElectoratesMeta, saveElectoratesMeta } = require("../lib/electorates");

function makeElectoratesRouter(cfg) {
  const r = express.Router();

  r.post("/meta/electorates", (req, res) => {
    const { termKey, official_order, alphabetical_order } = req.body || {};
    if (!termKey || !official_order || !alphabetical_order) {
      return res.status(400).json({ ok: false, error: "Expected { termKey, official_order, alphabetical_order }" });
    }

    const meta = loadElectoratesMeta(cfg.ELECTORATES_BY_TERM_PATH);

    const cleanedOfficial = {};
    for (const [num, name] of Object.entries(official_order)) {
      const n = Number(num);
      if (!Number.isFinite(n) || n <= 0 || !Number.isInteger(n)) continue;
      const clean = cleanElectorateName(name);
      if (clean) cleanedOfficial[String(n)] = clean;
    }

    const names = Object.values(cleanedOfficial);
    const alpha = [...names].sort((a, b) => a.localeCompare(b, "en", { sensitivity: "base" }));
    const rebuiltAlpha = {};
    alpha.forEach((nm, i) => (rebuiltAlpha[nm] = i + 1));

    meta[termKey] = { official_order: cleanedOfficial, alphabetical_order: rebuiltAlpha };
    saveElectoratesMeta(cfg.ELECTORATES_BY_TERM_PATH, meta);

    appendJsonl(cfg.LOG_ELECTORATES_INGEST, {
      ts: new Date().toISOString(),
      termKey,
      count: Object.keys(cleanedOfficial).length,
    });

    res.json({ ok: true, termKey, count: Object.keys(cleanedOfficial).length });
  });

  r.get("/meta/electorates", (_req, res) => res.json(loadElectoratesMeta(cfg.ELECTORATES_BY_TERM_PATH)));

  r.post("/meta/electorates/reset", (_req, res) => {
    saveElectoratesMeta(cfg.ELECTORATES_BY_TERM_PATH, {});
    appendJsonl(cfg.LOG_ELECTORATES_INGEST, { ts: new Date().toISOString(), action: "reset" });
    res.json({ ok: true });
  });

  return r;
}

module.exports = { makeElectoratesRouter };
