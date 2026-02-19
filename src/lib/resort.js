const fs = require("fs");
const path = require("path");
const os = require("os");


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

function ensureUniqueTarget(targetAbs, strategy) {
  if (!fs.existsSync(targetAbs)) return targetAbs;
  if (strategy === "overwrite") return targetAbs;
  if (strategy === "skip") return null;
  // default: suffix
  const dir = path.dirname(targetAbs);
  const ext = path.extname(targetAbs);
  const base = path.basename(targetAbs, ext);
  for (let i = 1; i < 1000; i++) {
    const cand = path.join(dir, `${base}__dup${i}${ext}`);
    if (!fs.existsSync(cand)) return cand;
  }
  return null;
}

function moveFile(oldAbs, newAbs, overwrite) {
  ensureDir(path.dirname(newAbs));
  if (overwrite && fs.existsSync(newAbs)) {
    try { fs.unlinkSync(newAbs); } catch { }
  }
  try {
    fs.renameSync(oldAbs, newAbs);
  } catch {
    fs.copyFileSync(oldAbs, newAbs);
    try { fs.unlinkSync(oldAbs); } catch { }
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

function sha256File(absPath) {
  const crypto = require("crypto");
  const h = crypto.createHash("sha256");
  const buf = fs.readFileSync(absPath);
  h.update(buf);
  return h.digest("hex");
}

/**
 * Resort already-downloaded files using routing.js.
 *
 * Uses downloaded_hash_index.json as the authoritative content list.
 * For each hash record:
 *  - compute desired path based on best source observation (url + source_page_url)
 *  - preserve filename using filenameOverride = basename(existing.saved_to)
 *  - move the canonical file if needed
 *  - update saved_to / termKey / electorateFolder / ext
 *  - update per-level manifests that point to old saved_to
 */
async function resortDownloads({ cfg, downloadsRootOverride, dryRun = true, conflict = "suffix", limit = null }) {
  const downloadsRoot = downloadsRootOverride ? path.resolve(downloadsRootOverride) : cfg.DOWNLOADS_ROOT;
  ensureDir(downloadsRoot);

  const electoratesByTerm = loadElectoratesMeta(cfg.ELECTORATES_BY_TERM_PATH);
  const idx = readJsonSafe(cfg.DOWNLOADED_HASH_INDEX_PATH, {});

  // Reverse index: saved_to (relative) -> sha256
  // Used to dedupe during resort when a target path already exists.
  const savedToSha = new Map();
  for (const [h, r] of Object.entries(idx)) {
    const p = r && typeof r === "object" ? r.saved_to : null;
    if (p) savedToSha.set(p, h);
  }

  const actions = [];
  let processed = 0;

  for (const [sha256, rec] of Object.entries(idx)) {
    if (limit && processed >= limit) break;
    processed++;

    if (!rec || typeof rec !== "object") continue;
    const oldRel = rec.saved_to;
    if (!oldRel) continue;
    const oldAbs = toAbsolute(oldRel);
    const st = safeStat(oldAbs);
    if (!st || !st.isFile()) {
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

    const newAbsDesired = route.outPath;
    const newRelDesired = toRelative(newAbsDesired);
    let forceTargetAbs = null;

    const samePath = path.resolve(oldAbs) === path.resolve(newAbsDesired);
    if (samePath) {
      rec.termKey = route.termKey;
      rec.electorateFolder = route.electorateFolder || null;
      rec.ext = route.ext;
      continue;
    }

    // Target exists handling (dedupe / canonical conflict)
    if (fs.existsSync(newAbsDesired)) {
      const existingRel = newRelDesired;

      // Try to identify what SHA "owns" the existing target.
      // Prefer index reverse map; if absent, hash the file on disk and see if it's indexed.
      let existingSha = savedToSha.get(existingRel) || null;

      if (!existingSha) {
        try {
          const diskSha = sha256File(newAbsDesired);
          if (idx[diskSha]) {
            existingSha = diskSha;
            // adopt into reverse map so later checks are cheap
            savedToSha.set(existingRel, existingSha);
          }
        } catch {
          // If we cannot hash, treat as unknown occupant and fall through to suffix strategy.
          existingSha = null;
        }
      }

      // (A) SAME-SHA: canonical dedupe (no dup suffix)
      if (existingSha && existingSha === sha256) {
        actions.push({
          action: dryRun ? "would_dedupe" : "dedupe",
          sha256,
          from: oldRel,
          to: existingRel,
          reason: "target_exists_same_sha",
        });

        const hashShort = String(sha256 || "").slice(0, 8);
        const tag = dryRun ? "[DRY]" : "[MOVE]";
        console.log(`${tag} DEDUPE ${hashShort}… ${oldRel}${os.EOL}           -> ${existingRel}`);

        if (!dryRun) {
          try { fs.unlinkSync(oldAbs); } catch { }

          // Point this record at the canonical existing location.
          rec.saved_to = existingRel;
          rec.termKey = route.termKey;
          rec.electorateFolder = route.electorateFolder || null;
          rec.ext = route.ext;
          rec.last_seen_ts = new Date().toISOString();
          if (!rec.first_seen_ts) rec.first_seen_ts = rec.last_seen_ts;

          updateLevelManifests(cfg, oldRel, existingRel, sha256);

          savedToSha.set(existingRel, sha256);
          savedToSha.delete(oldRel);
        }

        continue; // skip normal conflict suffix logic
      }

      // (B) DIFFERENT-SHA: canonical-to-index wins the canonical filename
      //
      // Rules:
      // 1) If existing file is canonical to index at this path -> incoming becomes __dupN
      // 2) If incoming file is canonical to index at this path -> existing gets renamed to __dupN, incoming takes canonical
      // 3) If neither is canonical -> keep existing canonical name, suffix incoming (safe)
      // 4) If both claim canonical (shouldn't happen) -> keep existing, suffix incoming, log
      const existingCanonical =
        existingSha && idx[existingSha] && idx[existingSha].saved_to === existingRel;

      const incomingCanonical =
        idx[sha256] && idx[sha256].saved_to === existingRel;

      if (incomingCanonical && !existingCanonical) {
        // Existing occupant is NOT canonical; rename it away, then let incoming take canonical path.
        const overwrite = false;
        const existingAbs = newAbsDesired;

        // Move existing file out of the way with dup suffix
        const displacedAbs = ensureUniqueTarget(existingAbs, "suffix");
        if (!displacedAbs) {
          actions.push({ action: "conflict_skip", sha256, from: oldRel, to: existingRel, strategy: "suffix", reason: "no_dup_slot_for_displaced" });
          continue;
        }

        // IMPORTANT: tell later code to skip suffix logic
        forceTargetAbs = newAbsDesired;

        const displacedRel = toRelative(displacedAbs);

        actions.push({
          action: dryRun ? "would_displace_existing" : "displace_existing",
          sha256_existing: existingSha || null,
          from: existingRel,
          to: displacedRel,
          reason: "incoming_is_index_canonical",
        });

        const tag = dryRun ? "[DRY]" : "[MOVE]";
        console.log(`${tag} SWAP     ${sha256.slice(0, 8)}… ${oldRel}${os.EOL}           -> ${existingRel}`);
        console.log(`${tag} DISPLACE ${(existingSha || "unknown").slice(0, 8)}… ${existingRel}${os.EOL}           -> ${displacedRel}`);

        if (!dryRun) {
          moveFile(existingAbs, displacedAbs, overwrite);

          // If the displaced file was indexed (we knew its sha), update that record to its new path
          if (existingSha && idx[existingSha]) {
            idx[existingSha].saved_to = displacedRel;
            updateLevelManifests(cfg, existingRel, displacedRel, existingSha);
          }

          savedToSha.delete(existingRel);
          if (existingSha) savedToSha.set(displacedRel, existingSha);
        }

        // Now proceed with normal move logic into existingRel (no suffix).
        // (fall through; DO NOT continue)
      } else {
        // Existing is canonical OR incoming not canonical -> incoming should be dup-suffixed by normal conflict logic
        // (fall through to ensureUniqueTarget below)
      }
    }

    const overwrite = conflict === "overwrite";
    const targetAbs = forceTargetAbs || ensureUniqueTarget(newAbsDesired, conflict);
    if (!targetAbs) {
      actions.push({ action: "conflict_skip", sha256, from: oldRel, to: newRelDesired, strategy: conflict });
      continue;
    }

    const targetRel = toRelative(targetAbs);
    actions.push({
      action: dryRun ? "would_move" : "move",
      sha256,
      from: oldRel,
      to: targetRel,
      termKey: route.termKey,
      electorateFolder: route.electorateFolder || null,
    });

    // Print planned moves in dry-run (and also in apply mode, for traceability)
    const hashShort = String(sha256 || "").slice(0, 8);
    const tag = dryRun ? "[DRY]" : "[MOVE]";
    const isDupTarget = (targetRel !== newRelDesired);

    const verb = isDupTarget ? "DUP" : "MOVE";
    console.log(`${tag} ${verb} ${hashShort}… ${oldRel}${os.EOL}           -> ${targetRel}`);


    if (!dryRun) {
      moveFile(oldAbs, targetAbs, overwrite);
      rec.saved_to = targetRel;
      rec.termKey = route.termKey;
      rec.electorateFolder = route.electorateFolder || null;
      rec.ext = route.ext;
      rec.last_seen_ts = new Date().toISOString();
      if (!rec.first_seen_ts) rec.first_seen_ts = rec.last_seen_ts;
      updateLevelManifests(cfg, oldRel, targetRel, sha256);
    }
  }

  // Phase B: disk-first dedupe (handle files present on disk but not represented in the hash index)
  scanDownloadsDiskFirst({ cfg, downloadsRoot, dryRun, conflict, idx, savedToSha, actions });


  if (!dryRun) {
    writeJson(cfg.DOWNLOADED_HASH_INDEX_PATH, idx);
  }

  for (const a of actions) {
    appendJsonl(cfg.LOG_FILE_SAVES, { kind: "resort", dryRun, ...a, ts: new Date().toISOString() });
  }

  const moved = actions.filter((x) => x.action === "move").length;
  const would = actions.filter((x) => x.action === "would_move").length;
  const missing = actions.filter((x) => x.action === "missing").length;
  const conflictSkips = actions.filter((x) => x.action === "conflict_skip").length;

  console.log(`resort-downloads: processed=${processed} dryRun=${dryRun} conflict=${conflict}`);
  console.log(`  would_move=${would} moved=${moved} missing=${missing} conflict_skip=${conflictSkips}`);
}

function walkFilesRec(dirAbs, out = []) {
  let entries;
  try {
    entries = fs.readdirSync(dirAbs, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const ent of entries) {
    const p = path.join(dirAbs, ent.name);
    if (ent.isDirectory()) walkFilesRec(p, out);
    else if (ent.isFile()) out.push(p);
  }
  return out;
}

function sha256FileAbs(absPath) {
  const crypto = require("crypto");
  const h = crypto.createHash("sha256");
  const buf = fs.readFileSync(absPath);
  h.update(buf);
  return h.digest("hex");
}

/**
 * Disk-first dedupe scan:
 * - For every file physically present under downloadsRoot:
 *   - compute sha256
 *   - if sha exists in idx -> dedupe file into idx[sha].saved_to (canonical)
 *   - else -> rename with __dupN (unknown content must not silently masquerade as canonical)
 */
function scanDownloadsDiskFirst({ cfg, downloadsRoot, dryRun, conflict, idx, savedToSha, actions }) {
  const filesOnDisk = walkFilesRec(downloadsRoot, []);
  if (!filesOnDisk.length) return;

  // Build a set of indexed on-disk locations (so we can ignore already-tracked files)
  const indexedAbs = new Set();
  for (const rec of Object.values(idx)) {
    const rel = rec && typeof rec === "object" ? rec.saved_to : null;
    if (!rel) continue;
    const abs = toAbsolute(rel);
    indexedAbs.add(path.resolve(abs));
  }

  for (const abs of filesOnDisk) {
    const absN = path.resolve(abs);

    // Skip files already represented in the index by location.
    if (indexedAbs.has(absN)) continue;

    const rel = toRelative(absN);
    let sha = null;

    try {
      sha = sha256FileAbs(absN);
    } catch (e) {
      actions.push({ action: "disk_hash_failed", saved_to: rel, error: String(e) });
      continue;
    }

    const hashShort = String(sha || "").slice(0, 8);
    const tag = dryRun ? "[DRY]" : "[MOVE]";

    // CASE 1: content exists in index -> dedupe into canonical saved_to
    if (idx[sha]) {
      const rec = idx[sha];
      const canonicalRel = rec.saved_to;

      // If index record is missing a saved_to, treat this disk file as canonical (adopt it)
      if (!canonicalRel) {
        actions.push({
          action: dryRun ? "would_adopt_unindexed" : "adopt_unindexed",
          sha256: sha,
          from: rel,
          to: rel,
          reason: "index_missing_saved_to",
        });
        console.log(`${tag} ADOPT  ${hashShort}… ${rel}`);
        if (!dryRun) {
          rec.saved_to = rel;
          savedToSha.set(rel, sha);
        }
        continue;
      }

      const canonicalAbs = toAbsolute(canonicalRel);

      // If canonical file exists, delete this duplicate copy
      if (fs.existsSync(canonicalAbs)) {
        actions.push({
          action: dryRun ? "would_dedupe_disk" : "dedupe_disk",
          sha256: sha,
          from: rel,
          to: canonicalRel,
          reason: "disk_file_hash_in_index",
        });
        console.log(`${tag} DEDUPE ${hashShort}… ${rel}${os.EOL}           -> ${canonicalRel}`);
        if (!dryRun) {
          try { fs.unlinkSync(absN); } catch { }
        }
        continue;
      }

      // Canonical path missing on disk: promote this disk file into canonical location
      actions.push({
        action: dryRun ? "would_promote_disk" : "promote_disk",
        sha256: sha,
        from: rel,
        to: canonicalRel,
        reason: "canonical_missing_promote_disk_copy",
      });
      console.log(`${tag} PROMOTE ${hashShort}… ${rel}${os.EOL}           -> ${canonicalRel}`);

      if (!dryRun) {
        const overwrite = conflict === "overwrite";
        const targetAbs = ensureUniqueTarget(canonicalAbs, overwrite ? "overwrite" : "suffix") || canonicalAbs;
        moveFile(absN, targetAbs, overwrite);
        const targetRel = toRelative(targetAbs);
        rec.saved_to = targetRel;
        savedToSha.set(targetRel, sha);
      }

      continue;
    }

    // CASE 2: content not in index -> unknown file
    // Only apply __dupN if there is a "twin" (a competing identity).

    // Define identity = filename without trailing __dupN
    const ext = path.extname(absN);
    const dir = path.dirname(absN);

    let base = path.basename(absN, ext);
    base = base.replace(/__dup\d+$/i, ""); // strip existing dup suffix for identity

    // Twin condition A: there is a sibling file with same identity but different filename
    let hasSiblingTwin = false;
    try {
      const siblings = fs.readdirSync(dir);
      for (const f of siblings) {
        if (f === path.basename(absN)) continue;
        const fExt = path.extname(f);
        const fBase = path.basename(f, fExt).replace(/__dup\d+$/i, "");
        if (fExt.toLowerCase() === ext.toLowerCase() && fBase === base) {
          hasSiblingTwin = true;
          break;
        }
      }
    } catch { }

    // Twin condition B: routing would move it to a path that is already occupied
    // (If you don't have a routed target for unknowns, you can skip this one.)
    let hasTargetTwin = false;
    // If you have a "desiredRel" computation available for disk-unknowns, set hasTargetTwin=true when occupied.
    // Otherwise just rely on sibling twin.
    if (!hasSiblingTwin && hasTargetTwin === false) {
      // No competing identity -> leave it alone
      actions.push({
        action: "leave_unindexed_no_twin",
        sha256: sha,
        saved_to: rel,
        reason: "unindexed_no_competing_twin",
      });
      // Optional: print for debugging (comment out if too noisy)
      // console.log(`${tag} KEEP   ${hashShort}… ${rel}  (no twin)`);
      continue;
    }

    // If we got here, it has a twin -> apply dup suffix
    let newAbs = null;
    for (let i = 1; i < 1000; i++) {
      const cand = path.join(dir, `${base}__dup${i}${ext}`);
      if (!fs.existsSync(cand)) {
        newAbs = cand;
        break;
      }
    }

    if (!newAbs) {
      actions.push({ action: "disk_unindexed_no_slot", saved_to: rel, sha256: sha });
      console.log(`${tag} UNK    ${hashShort}… ${rel}  (no dup slot available)`);
      continue;
    }

    const newRel = toRelative(newAbs);

    actions.push({
      action: dryRun ? "would_dup_unindexed" : "dup_unindexed",
      sha256: sha,
      from: rel,
      to: newRel,
      reason: "disk_file_hash_not_in_index_has_twin",
    });

    console.log(`${tag} DUP    ${hashShort}… ${rel}${os.EOL}           -> ${newRel}`);

    if (!dryRun) {
      try {
        fs.renameSync(absN, newAbs);
      } catch {
        fs.copyFileSync(absN, newAbs);
        try { fs.unlinkSync(absN); } catch { }
      }
    }
  }
}

module.exports = { resortDownloads };
