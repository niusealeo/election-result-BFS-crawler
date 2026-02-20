// Simple structured console logger with ISO and NZDT timestamps.
//
// We keep this dependency-free so the sink can run anywhere.
// ISO timestamps are UTC; nzdt gives a human-friendly Pacific/Auckland time.

function nowTimestamps() {
  const d = new Date();
  const iso = d.toISOString();
  let nzdt;
  try {
    nzdt = new Intl.DateTimeFormat("en-NZ", {
      timeZone: "Pacific/Auckland",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      timeZoneName: "short",
    }).format(d);
  } catch {
    nzdt = d.toString();
  }
  return { iso, nzdt };
}

function logEvent(type, details) {
  const { iso, nzdt } = nowTimestamps();
  const t = String(type || "EVENT").toUpperCase();
  console.log(`[${iso}] [${nzdt}] ${t}`);
  if (details && typeof details === "object") {
    for (const [k, v] of Object.entries(details)) {
      if (v === undefined) continue;
      const val = (v && typeof v === "object") ? JSON.stringify(v) : String(v);
      console.log(`  ${k}: ${val}`);
    }
  }
}

module.exports = { logEvent, nowTimestamps };
