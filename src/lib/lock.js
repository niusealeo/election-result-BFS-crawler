// Simple in-process mutex (promise chain).
// Serializes critical read-modify-write sections to avoid lost updates
// when multiple Postman runs hit the sink concurrently.
let chain = Promise.resolve();

function withLock(fn) {
  const run = () => Promise.resolve().then(fn);
  const next = chain.then(run, run);
  chain = next.catch(() => {});
  return next;
}

module.exports = { withLock };
