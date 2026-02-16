function sniffIsPdf(buf) {
  if (!buf || buf.length < 5) return false;
  return buf[0] === 0x25 && buf[1] === 0x50 && buf[2] === 0x44 && buf[3] === 0x46 && buf[4] === 0x2d;
}

function looksLikeHtml(buf) {
  const head = buf.slice(0, 512).toString("utf8").trim().toLowerCase();
  return head.startsWith("<!doctype html") || head.startsWith("<html") || head.includes("<head") || head.includes("<title");
}

module.exports = { sniffIsPdf, looksLikeHtml };
