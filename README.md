# Sink — a homecooked Postman API (BFS crawl + file downloader)

This “sink” is a small local Node/Express API that exists purely to support our Postman Collection Runner workflow.

Postman is doing the network work (fetch pages, download files), but Postman is bad at persistent state across many iterations and levels. The sink is our final dedupe net and our local filesystem writer, so the crawl stays deterministic and restartable.

Link to Postman collections runner workspace here: https://www.postman.com/willscire/bfs-election-crawler/collection/2650823-fed29af4-fd55-499b-b123-624e5fc80c03

Think of it like:
Postman = crawler client  
Sink = crawl brain + disk writer

---

## What this API does

### 1) BFS dedupe net (frontier builder)
Endpoint: POST /dedupe/level

Postman discovers:
- visited pages (pages fetched this run)
- pages (new page URLs found)
- files (downloadable file URLs found)

The sink:
- normalizes URLs
- dedupes by BFS rules
- writes next-level URL list (urls-level-(L+1).json)
- writes current-level file list (files-level-L.json)

BFS identity rules:
- Page URLs: deduped by normalized URL
- File URLs: deduped by normalized file URL
- source_page_url is metadata only

---

### 2) File writer + router
Endpoint: POST /upload/file

Postman downloads file bytes and sends base64 content to the sink.

Files are saved into:

downloads/<termKey>/<electorateFolder?>/<filename>

No extra /pdf/ directory is used.

If a PDF response is actually HTML, it is saved to:

downloads/<termKey>/_bad/

---

### 3) Electorates metadata store
Endpoint: POST /meta/electorates

Stores electorate numbering per term so file routing can place files into the correct folder.

---

## Directory layout

BFS_crawl/
  runs/
  _meta/
    state.json
    electorates_by_term.json
    artifacts/
      urls-level-<L>.json
      files-level-<L>.json

downloads/
  term_.../

---

## Artifact JSON format (meta-first row)

Default format uses a conflated _meta first row:

urls-level-(L+1).json
[
  { "_meta": true, "level": 2, "kind": "urls", "url": "https://example/pageA" },
  { "url": "https://example/pageB" }
]

files-level-L.json
[
  { "_meta": true, "level": 1, "kind": "files", "url": "https://example/file.pdf", "ext": "pdf", "source_page_url": "https://example/pageA" },
  { "url": "https://example/file2.xlsx", "ext": "xlsx", "source_page_url": "https://example/pageB" }
]

Disable with:
ARTIFACT_META_FIRST_ROW=0

---

## Running

npm install
npm start

Health check:
GET http://localhost:3000/health

---

## Why this exists

Postman is the crawler.  
Sink is the brain and filesystem.
