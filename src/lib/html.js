// Minimal HTML entity decoder for electorate names like "M&#257;ngere".
// Supports: numeric decimal (&#333;), numeric hex (&#x14D;), and a few common named entities.

function decodeHtmlEntities(input) {
  let s = String(input ?? "");
  if (!s) return s;

  // named entities we actually see / care about in this project
  const named = {
    amp: "&",
    lt: "<",
    gt: ">",
    quot: '"',
    apos: "'",
    nbsp: " ",
  };

  // numeric hex: &#x14D;
  s = s.replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => {
    const cp = parseInt(hex, 16);
    if (!Number.isFinite(cp)) return _;
    try {
      return String.fromCodePoint(cp);
    } catch {
      return _;
    }
  });

  // numeric decimal: &#333;
  s = s.replace(/&#(\d+);/g, (_, dec) => {
    const cp = parseInt(dec, 10);
    if (!Number.isFinite(cp)) return _;
    try {
      return String.fromCodePoint(cp);
    } catch {
      return _;
    }
  });

  // named: &amp;
  s = s.replace(/&([a-zA-Z]+);/g, (m, name) => {
    const k = String(name).toLowerCase();
    return Object.prototype.hasOwnProperty.call(named, k) ? named[k] : m;
  });

  // normalize to NFC so macrons are consistent
  try {
    s = s.normalize("NFC");
  } catch {}

  return s;
}

module.exports = { decodeHtmlEntities };
