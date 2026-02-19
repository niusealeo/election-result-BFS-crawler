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
    try {
      fs.unlinkSync(newAbs);
    } catch {}
  }
  try {
    fs.renameSync(oldAbs, newAbs);
  } catch {
    fs.copyFileSync(oldAbs, newAbs);
    try {
      fs.unlinkSync(oldAbs);
    } catch {}
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
  // Used to identify who "owns" a path.
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

    const samePath = path.resolve(oldAbs) === path.resolve(newAbsDesired);
    if (samePath) {
      rec.termKey = route.termKey;
      rec.electorateFolder = route.electorateFolder || null;
      rec.ext = route.ext;
      continue;
    }

    // If the desired target is already occupied, decide whether we should:
    // - DEDUPE (same content), or
    // - DISPLACE the occupant (when the incoming file is the indexed/canonical one), or
    // - SUFFIX the incoming file (when the occupant should keep the canonical name).
    //
    // Key rule for your workflow:
    //   If the incoming file's SHA256 is in downloaded_hash_index.json, it is canonical content.
    //   If the occupant is not indexed (or is indexed but "misplaced"), rename the occupant to __dupN
    //   and let the canonical file take the routed canonical filename.
    let forceTargetAbs = null;

    if (fs.existsSync(newAbsDesired)) {
      const existingRel = newRelDesired;               // toRelative(newAbsDesired)
      const existingAbs = newAbsDesired;
      const existingSha = savedToSha.get(existingRel); // who already "owns" this path?
      let diskSha = null;

      if (!existingSha) {
        // Target exists but not in index — hash it directly so we can see if it's indexed content.
        try {
          const crypto = require("crypto");
          const data = fs.readFileSync(existingAbs);
          diskSha = crypto.createHash("sha256").update(data).digest("hex");
        } catch {
          diskSha = null;
        }
      }

      const occupantSha = existingSha || diskSha || null;
      const occupantIsIndexed = occupantSha && idx[occupantSha] && typeof idx[occupantSha] === "object";

      // (A) SAME-SHA: dedupe (no dup suffix)
      if (occupantSha && occupantSha === sha256) {
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
          // Delete the redundant file, keep the one already at the target path.
          try { fs.unlinkSync(oldAbs); } catch {}

          // Update this record to point to the canonical existing location.
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

        continue; // Important: skip normal conflict suffix logic
      }

      // (B) DIFFERENT CONTENT: decide whether to displace the occupant
      //
      // Incoming file is always indexed content here (we are iterating downloaded_hash_index.json),
      // so it should take the canonical routed filename UNLESS the occupant is also indexed and
      // appears to "belong" at this canonical path.
      let shouldDisplaceOccupant = false;

      if (!occupantIsIndexed) {
        shouldDisplaceOccupant = true;
      } else {
        // Occupant is indexed. If it would NOT route to this same path based on its own sources,
        // treat it as misplaced and displace it (so each indexed SHA can land on its routed path).
        try {
          const occRec = idx[occupantSha];
          const occSrc = pickBestSource(occRec.sources);
          if (occSrc?.url) {
            const occFilenameOverride = path.basename(existingRel);
            const occRoute = resolveSavePath({
              downloadsRoot,
              url: occSrc.url,
              ext: occRec.ext || null,
              source_page_url: occSrc.source_page_url || null,
              electoratesByTerm,
              filenameOverride: occFilenameOverride,
            });
            const occDesiredRel = toRelative(occRoute.outPath);
            if (occDesiredRel !== existingRel) {
              shouldDisplaceOccupant = true;
            }
          }
        } catch {
          // If we cannot evaluate occupant routing, keep it conservative (don't displace).
          shouldDisplaceOccupant = false;
        }
      }

      if (shouldDisplaceOccupant) {
        const displacedAbs = ensureUniqueTarget(existingAbs, "suffix"); // will return __dupN
        if (!displacedAbs) {
          actions.push({
            action: "conflict_skip",
            sha256,
            from: oldRel,
            to: existingRel,
            strategy: "suffix",
            reason: "no_dup_slot_for_displaced_occupant",
          });
          continue;
        }

        const displacedRel = toRelative(displacedAbs);

        actions.push({
          action: dryRun ? "would_displace" : "displace",
          sha256_incoming: sha256,
          sha256_occupant: occupantSha || null,
          from: existingRel,
          to: displacedRel,
          reason: occupantIsIndexed ? "occupant_indexed_but_misplaced" : "occupant_not_indexed",
        });

        const tag = dryRun ? "[DRY]" : "[MOVE]";
        console.log(`${tag} DISPLACE ${(occupantSha || "unknown").slice(0, 8)}… ${existingRel}${os.EOL}           -> ${displacedRel}`);

        if (!dryRun) {
          // Move the occupant out of the way
          moveFile(existingAbs, displacedAbs, false);

          // If the occupant was indexed, update its record to the displaced path so it won't go "missing"
          if (occupantIsIndexed) {
            idx[occupantSha].saved_to = displacedRel;
            updateLevelManifests(cfg, existingRel, displacedRel, occupantSha);
            savedToSha.set(displacedRel, occupantSha);
          }
          savedToSha.delete(existingRel);
        }

        // Now the canonical target name is available for the incoming file.
        forceTargetAbs = existingAbs;
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

    const hashShort = String(sha256 || "").slice(0, 8);
    const tag = dryRun ? "[DRY]" : "[MOVE]";
    console.log(`${tag} MOVE ${hashShort}… ${oldRel}${os.EOL}           -> ${targetRel}`);

    if (!dryRun) {
      moveFile(oldAbs, targetAbs, overwrite);
      rec.saved_to = targetRel;
      rec.termKey = route.termKey;
      rec.electorateFolder = route.electorateFolder || null;
      rec.ext = route.ext;
      rec.last_seen_ts = new Date().toISOString();
      if (!rec.first_seen_ts) rec.first_seen_ts = rec.last_seen_ts;
      updateLevelManifests(cfg, oldRel, targetRel, sha256);

      // Update reverse map
      savedToSha.set(targetRel, sha256);
      savedToSha.delete(oldRel);
    }
  }

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

module.exports = { resortDownloads };
