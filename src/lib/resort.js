const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");

const { resolveSavePath } = require("./routing");
const { loadElectoratesMeta } = require("./electorates");
const { ensureDir, readJsonSafe, writeJson } = require("./fsx");
const { appendJsonl } = require("./jsonl");
const { toAbsolute, toRelative } = require("./paths");

function safeStat(absPath) {
  try {
    return fs.statSync(absPath);
  } catch {
    return null;
  }
}

function sha256File(absPath) {
  // Files are typically not huge here (xls/csv/pdf). Sync is fine for CLI.
  const h = crypto.createHash("sha256");
  const buf = fs.readFileSync(absPath);
  h.update(buf);
  return h.digest("hex");
}

function ensureUniqueTarget(targetAbs) {
  // Always suffix in-place with __dupN.
  const dir = path.dirname(targetAbs);
  const ext = path.extname(targetAbs);
  const base = path.basename(targetAbs, ext);
  for (let i = 1; i < 1000; i++) {
    const cand = path.join(dir, `${base}__dup${i}${ext}`);
    if (!fs.existsSync(cand)) return cand;
  }
  return null;
}

function moveFile(oldAbs, newAbs, overwrite = false) {
  ensureDir(path.dirname(newAbs));
  if (overwrite && fs.existsSync(newAbs)) {
    try { fs.unlinkSync(newAbs); } catch {}
  }
  try {
    fs.renameSync(oldAbs, newAbs);
  } catch {
    fs.copyFileSync(oldAbs, newAbs);
    try { fs.unlinkSync(oldAbs); } catch {}
  }
}

function pickBestSource(sources) {
  if (!Array.isArray(sources) || sources.length === 0) return null;
  // Prefer most recent ts; else first
  let best = sources[0];
  let bestT = Date.parse(best?.ts || "");
  for (const s of sources) {
    const t = Date.parse(s?.ts || "");
    if (Number.isFinite(t) && (Number.isNaN(bestT) || t > bestT)) {
      best = s;
      bestT = t;
    }
  }
  return best;
}

function updateLevelManifests(cfg, oldRel, newRel, sha256) {
  if (!fs.existsSync(cfg.LEVEL_FILES_DIR)) return;
  const files = fs.readdirSync(cfg.LEVEL_FILES_DIR).filter((f) => f.endsWith(".json"));
  for (const f of files) {
    const p = path.join(cfg.LEVEL_FILES_DIR, f);
    const m = readJsonSafe(p, null);
    if (!m || !Array.isArray(m.files)) continue;
    let changed = false;
    for (const ent of m.files) {
      if (!ent) continue;
      if (sha256 && ent.sha256 !== sha256) continue;
      if (ent.saved_to === oldRel) {
        ent.saved_to = newRel;
        changed = true;
      }
    }
    if (changed) writeJson(p, m);
  }
}

/**
 * Resort already-downloaded files using routing.js.
 *
 * Authoritative input: downloaded_hash_index.json (per domain).
 *
 * Behaviours:
 *  - Move/rename indexed files into their routed canonical location.
 *  - If canonical target is occupied:
 *      * same sha => dedupe (adopt canonical path).
 *      * different sha:
 *          - if occupant sha is NOT in index: displace occupant to __dupN, then place indexed file.
 *          - if occupant sha IS in index: do not overwrite; suffix this file to __dupN (twin).
 */
async function resortDownloads({ cfg, downloadsRootOverride, dryRun = true, conflict = "suffix", limit = null }) {
  const downloadsRoot = downloadsRootOverride ? path.resolve(downloadsRootOverride) : cfg.DOWNLOADS_ROOT;
  ensureDir(downloadsRoot);

  const electoratesByTerm = loadElectoratesMeta(cfg.ELECTORATES_BY_TERM_PATH);
  const idx = readJsonSafe(cfg.DOWNLOADED_HASH_INDEX_PATH, {});

  const actions = [];
  let processed = 0;

  // Counters for sanity
  let wouldMove = 0;
  let moved = 0;
  let missing = 0;
  let conflictSkip = 0;
  let wouldDedupe = 0;
  let deduped = 0;
  let wouldDup = 0;
  let duped = 0;
  let wouldDisplace = 0;
  let displaced = 0;
  let diskHashFailed = 0;

  for (const [sha256, rec] of Object.entries(idx)) {
    if (limit && processed >= limit) break;
    processed++;

    if (!rec || typeof rec !== "object") continue;
    const oldRel = rec.saved_to;
    if (!oldRel) continue;

    const oldAbs = toAbsolute(oldRel);
    const st = safeStat(oldAbs);
    if (!st || !st.isFile()) {
      missing++;
      actions.push({ action: "missing", sha256, saved_to: oldRel });
      continue;
    }

    const src = pickBestSource(rec.sources);
    if (!src?.url) {
      actions.push({ action: "no_source", sha256, saved_to: oldRel });
      continue;
    }

    const filenameOverride = path.basename(oldRel);
    const route = resolveSavePath({
      downloadsRoot,
      url: src.url,
      ext: rec.ext || null,
      source_page_url: src.source_page_url || null,
      electoratesByTerm,
      filenameOverride,
    });

    const desiredAbs = route.outPath;
    const desiredRel = toRelative(desiredAbs);
    const samePath = path.resolve(oldAbs) === path.resolve(desiredAbs);

    // Always refresh routing metadata even if no move.
    rec.termKey = route.termKey;
    rec.electorateFolder = route.electorateFolder || null;
    rec.ext = route.ext;

    if (samePath) continue;

    const hashShort = String(sha256 || "").slice(0, 8);

    // Fast path: empty target
    if (!fs.existsSync(desiredAbs)) {
      actions.push({ action: dryRun ? "would_move" : "move", sha256, from: oldRel, to: desiredRel });
      console.log(`${dryRun ? "[DRY]" : "[MOVE]"} MOVE ${hashShort}… ${oldRel}${os.EOL}           -> ${desiredRel}`);
      if (dryRun) {
        wouldMove++;
      } else {
        moveFile(oldAbs, desiredAbs, false);
        rec.saved_to = desiredRel;
        rec.last_seen_ts = new Date().toISOString();
        if (!rec.first_seen_ts) rec.first_seen_ts = rec.last_seen_ts;
        updateLevelManifests(cfg, oldRel, desiredRel, sha256);
        moved++;
      }
      continue;
    }

    // Occupied target: compare content
    let occSha = null;
    try {
      occSha = sha256File(desiredAbs);
    } catch {
      diskHashFailed++;
      occSha = null;
    }

    if (occSha && occSha === sha256) {
      // Same content already at destination: dedupe by adopting canonical path.
      actions.push({ action: dryRun ? "would_dedupe" : "dedupe", sha256, from: oldRel, to: desiredRel });
      console.log(`${dryRun ? "[DRY]" : "[APPLY]"} DEDUPE ${hashShort}… ${oldRel}${os.EOL}             => ${desiredRel}`);
      if (dryRun) {
        wouldDedupe++;
      } else {
        // Delete the old file (it is redundant) and update index to canonical path.
        try { fs.unlinkSync(oldAbs); } catch {}
        rec.saved_to = desiredRel;
        rec.last_seen_ts = new Date().toISOString();
        if (!rec.first_seen_ts) rec.first_seen_ts = rec.last_seen_ts;
        updateLevelManifests(cfg, oldRel, desiredRel, sha256);
        deduped++;
      }
      continue;
    }

    const occIndexed = occSha && Boolean(idx[occSha]);

    // If occupant is indexed, we do NOT overwrite it. We suffix THIS file (twin).
    if (occIndexed || conflict === "skip") {
      if (conflict === "skip") {
        conflictSkip++;
        actions.push({ action: "conflict_skip", sha256, from: oldRel, to: desiredRel });
        console.log(`[SKIP] CONFLICT ${hashShort}… ${oldRel}${os.EOL}           !! ${desiredRel} occupied`);
        continue;
      }

      const dupAbs = ensureUniqueTarget(desiredAbs);
      if (!dupAbs) {
        conflictSkip++;
        actions.push({ action: "conflict_skip", sha256, from: oldRel, to: desiredRel, note: "no_dup_slot" });
        continue;
      }
      const dupRel = toRelative(dupAbs);

      actions.push({ action: dryRun ? "would_dup" : "dup", sha256, from: oldRel, to: dupRel, canonical_conflict: true });
      console.log(`${dryRun ? "[DRY]" : "[DUP]"} DUP ${hashShort}… ${oldRel}${os.EOL}          -> ${dupRel}`);
      if (dryRun) {
        wouldDup++;
      } else {
        moveFile(oldAbs, dupAbs, false);
        rec.saved_to = dupRel;
        rec.last_seen_ts = new Date().toISOString();
        if (!rec.first_seen_ts) rec.first_seen_ts = rec.last_seen_ts;
        updateLevelManifests(cfg, oldRel, dupRel, sha256);
        duped++;
      }
      continue;
    }

    // Occupant is NOT indexed: displace it to dup, then place indexed file at canonical path.
    const displaceAbs = ensureUniqueTarget(desiredAbs);
    if (!displaceAbs) {
      conflictSkip++;
      actions.push({ action: "conflict_skip", sha256, from: oldRel, to: desiredRel, note: "no_displace_slot" });
      continue;
    }
    const displaceRel = toRelative(displaceAbs);

    actions.push({ action: dryRun ? "would_displace" : "displace", sha256, occupied_by_sha256: occSha, displaced_to: displaceRel });
    actions.push({ action: dryRun ? "would_move" : "move", sha256, from: oldRel, to: desiredRel, after_displace: true });

    console.log(`${dryRun ? "[DRY]" : "[APPLY]"} DISPLACE ${hashShort}… ${desiredRel}${os.EOL}           -> ${displaceRel}`);
    console.log(`${dryRun ? "[DRY]" : "[MOVE]"} MOVE ${hashShort}… ${oldRel}${os.EOL}           -> ${desiredRel}`);

    if (dryRun) {
      wouldDisplace++;
      wouldMove++;
    } else {
      moveFile(desiredAbs, displaceAbs, false);
      displaced++;
      moveFile(oldAbs, desiredAbs, false);
      rec.saved_to = desiredRel;
      rec.last_seen_ts = new Date().toISOString();
      if (!rec.first_seen_ts) rec.first_seen_ts = rec.last_seen_ts;
      updateLevelManifests(cfg, oldRel, desiredRel, sha256);
      moved++;
    }
  }

  if (!dryRun) {
    writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);
  }

  for (const a of actions) {
    appendJsonl(cfg.LOG_FILE_SAVES, { kind: "resort", dryRun, ...a, ts: new Date().toISOString() });
  }

  console.log(`resort-downloads: processed=${processed} dryRun=${dryRun} conflict=${conflict}`);
  console.log(`  would_move=${wouldMove} moved=${moved} missing=${missing} conflict_skip=${conflictSkip}`);
  console.log(`  would_dedupe=${wouldDedupe} deduped=${deduped}`);
  console.log(`  would_dup=${wouldDup} duped=${duped}`);
  console.log(`  would_displace=${wouldDisplace} displaced=${displaced}`);
  console.log(`  disk_hash_failed=${diskHashFailed}`);
}

module.exports = { resortDownloads };
