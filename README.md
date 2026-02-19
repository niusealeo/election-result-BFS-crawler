# Sink — a homecooked Postman API (BFS crawl + file downloader)

The core purpose of this repo was originally to download a curated copy of MMP election result files from NZ election websites. That purpose has now been satisfied, and to reduce load on the Electoral Commission's website servers the results for 1996-2025 can now be downloaded in a compact zip from OneDrive storage [here](https://1drv.ms/f/c/6262763291824deb/IgClU6VQnW9JSq52bHpU8fAaAUbLn-Zjw5YLe2Cwrr0L54I?e=Im63U0).
To check for subsequent updates with this repo, the election servers can be rescanned for new pages and pinged with a meta probe to check for file diffs.

The main branch of this repo has since evolved into a more generic purpose semi-automatic webcrawler that can be manually used to statefully index the web layout of any website domain pages and their associated exposed file urls, and identifying any diffs or updates during crawling reruns.

This “sink” is a small local Node/Express API that exists purely in tandem with a Postman Collection Runner workflow.

Link to Postman collections runner workspace here: https://www.postman.com/willscire/bfs-election-crawler/collection/2650823-fed29af4-fd55-499b-b123-624e5fc80c03

Think of it like:
Postman = crawler client  
(In)Sink(erator) = crawling results sorter + disk writer

---


## What this API does

### BFS dedupe net (frontier builder)
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

# Election Results BFS Crawler (Postman + Sink) — Domain-Scoped Edition

This package is a **domain-scoped** BFS (breadth-first search) crawler + download “sink” designed for websites that may:
- use **Cloudflare** or other anti-bot protections (so **Postman** performs outbound requests),
- expose files via **static HTML**, *but not* **JS-injected links**,
- publish **revisions** under the *same URL* (preliminary → final),
- require **re-sorting** after downloads as routing logic improves.

The workflow is intentionally split:
- **Postman** does all outbound HTTP requests (Discover / Probe Meta / Download).
- **Sink (Node.js server)** receives Postman results and maintains state (levels, diffs, indexes, routing, re-sorting).

---

## What you get

- BFS discovery of pages and file URLs, exported as **runner-friendly JSON**.
- Domain-scoped state under `_meta/<domain>/...` so multiple sites never pollute each other.
- Download tracking in a content-addressed **downloaded_hash_index.json**.
- Optional **meta probing** (HEAD or small GET) to detect changed files without downloading everything.
- A **resort** tool that can re-route already-downloaded files using your routing rules, with **dry-run** and `--apply`.
- Deterministic **duplicate/twin handling** based on SHA256 and index authority.

---

## Key design principles

### 1) Domain-scoped state
Every website (root domain) has its **own** state + downloads:

```
_meta/
  electionresults.govt.nz/
    state.json
    electorates_by_term.json
    electorates_by_term.jsonl
    downloaded_hash_index.json
    meta_probes.jsonl
    level_resets.jsonl
    file_saves.jsonl
    levels/
    diffs/

downloads/
  electionresults.govt.nz/
    term_51_(2014)/
    term_52_(2017)/
    ...
```

This allows you to:
- crawl multiple domains independently,
- re-run updates safely,
- keep all artifacts for a domain together.

### 2) Postman is stateless between runs (but stateful within a run)
You may use Postman variables during a **single collection run**, but the system must not rely on leftovers from other collections or workspaces.

Sink stores persistent state.

### 3) Content-addressed authority
The authoritative truth is:
- **SHA256** of downloaded files
- the `downloaded_hash_index.json` mapping SHA → canonical saved path + sources

Routing determines where a file *should* live, but the index determines which content is canonical.

---

## Installation

### Requirements
- Node.js 18+ (recommended)
- Postman (desktop)
- A local filesystem where downloads and `_meta` can be stored

### Install
From the package root:

```bash
npm install
```

---

## Running the sink (server)

Start the sink server (the HTTP endpoint Postman will call):

```bash
npm run start
```

Watch console output for the listening port (commonly `http://localhost:3030` or similar).

> If you run multiple domains, you still run **one** sink instance. The sink chooses the domain namespace per request.

---

## Domain selection rules (how the sink chooses `_meta/<domain>` and `downloads/<domain>`)

For each incoming Postman payload row, sink determines the domain key:

1. If `crawl_root` (or `root_url`) exists → domain = hostname(crawl_root)
2. Else if `url` exists → domain = hostname(url)
3. Else → domain = `default`

Hostname normalization:
- lowercased
- `www.` removed

Examples:
- `https://www.electionresults.govt.nz/...` → `electionresults.govt.nz`
- `https://example.org/path` → `example.org`

---

## The Postman pipeline (recommended)

You described a standard collection with:

1. **Step 0: Get Electorate Enumeration** (optional / domain-specific)
2. **Step 1: Discover Links**
3. **Step 2: Meta Probe (optional)**
4. **Step 3: Download Files**
5. **(Afterwards) Resort / Re-route locally**

### Step 0 — Get Electorate Enumeration (optional)
This is mainly for `electionresults.govt.nz`, to build `electorates_by_term.json`.
Send the resulting JSON to sink so routing can map electorate numbers to names.

Outputs stored under:

- `_meta/<domain>/electorates_by_term.json`
- `_meta/<domain>/electorates_by_term.jsonl` (log / ingest history)

### Step 1 — Discover Links (BFS)
Postman fetches pages (GET) and extracts links (href/src/etc) and emits URL rows to sink.

Sink writes:
- `_meta/<domain>/levels/urls-level-L.json`
- `_meta/<domain>/levels/files-level-L.json` (file candidates found at that level)
- `_meta/<domain>/state.json` (visited/frontier bookkeeping)

### Step 2 — Meta Probe (optional, recommended for updates)
Postman probes file URLs (typically HEAD; sometimes GET range if HEAD is blocked).

Sink writes:
- `_meta/<domain>/meta_probes.jsonl`

Sink can fold probe results into:
- `_meta/<domain>/diffs/files-diff-level-L.json`  (added/modified)
so you can download only changed files.

### Step 3 — Download Files
Postman downloads each file URL (GET) and streams bytes. Postman sends:
- url
- source_page_url
- level
- bytes (or saved content) / plus headers if available

Sink:
- saves the file under `downloads/<domain>/...` using routing
- computes SHA256
- updates `_meta/<domain>/downloaded_hash_index.json`
- appends `_meta/<domain>/file_saves.jsonl` (audit log)
- updates per-level file manifests if configured

---

## JSON artifacts (what they look like)

### `files-level-L.json`
Runner-friendly list of file URL rows discovered at level `L`.
Fields often include:
- `url`
- `source_page_url`
- `level`
- `ext` (optional)
- additional hints

### `files-diff-level-L.json`
Runner-friendly diff list (usually only the actionable subset):
- added
- modified (as per probe meta or by comparing index signatures)

Removed rows are not runner-useful for downloads and are typically excluded from runner diffs.

### `downloaded_hash_index.json`
Content-addressed index keyed by SHA256:

```json
{
  "<sha256>": {
    "saved_to": "downloads/<domain>/term_51_(2014)/by-elections/Mt_Albert_27.xls",
    "ext": "xls",
    "termKey": "term_51_(2014)",
    "electorateFolder": null,
    "sources": [
      {
        "url": "https://www.electionresults.govt.nz/2017_mt_albert_byelection/Mt_Albert_27.xls",
        "source_page_url": "https://www.electionresults.govt.nz/2017_mt_albert_byelection/",
        "level": 2,
        "ts": "2026-02-19T..."
      }
    ],
    "first_seen_ts": "...",
    "last_seen_ts": "..."
  }
}
```

**Important:** `saved_to` is a **relative path** from repo root:
- `downloads/<domain>/...`

### `file_saves.jsonl`
Append-only audit of actions:
- downloads
- resort moves
- dedupe
- displace/twin operations
- probe ingestion

---

## Routing and sorting

### Routing: `src/lib/routing.js`
Routing decides where a file should live based on:
- URL (and source_page_url)
- term inference logic
- electorate inference logic
- special cases (by-election, referendum buckets, etc.)

Outputs a target path:

```
downloads/<domain>/term_51_(2014)/by-elections/<filename>
downloads/<domain>/term_51_(2014)/referenda/<filename>
downloads/<domain>/term_52_(2017)/<electorateFolder>/<filename>
...
```

### Resort: re-sort already-downloaded files
Resort processes the authoritative index and fixes placement on disk.

#### Dry run
Prints planned changes without moving files:

```bash
node src/index.js resort-downloads --domain=electionresults.govt.nz
```

You should see lines like:

- **MOVE** (normal relocation)
- **DEDUPE** (same SHA already at target)
- **DISPLACE** (existing occupant gets renamed to `__dupN` to make way for canonical content)
- **DUP** (incoming is not canonical and gets a suffix)

#### Apply
Actually moves files and updates indexes/manifests:

```bash
node src/index.js resort-downloads --domain=electionresults.govt.nz --apply
```

### Twins / duplicates policy (critical)
When two files want the same canonical name:

- If the occupant has **same SHA** → redundant file is deleted (dedupe).
- If occupant is **different SHA**:
  - If occupant is **not indexed** (or indexed but misplaced) → occupant is displaced to `__dupN`
  - canonical indexed content takes the canonical filename.

This prevents silent overwrites while ensuring the index remains authoritative.

---

## Updates (baseline → update → update ...)

You can update incrementally:

1. Run Discover Links again (Step 1).
2. Sink updates the `levels` and computes diffs.
3. Optionally run Meta Probe (Step 2) to identify modified files.
4. Download only the diff lists (Step 3).
5. Run Resort to re-route after new logic improvements.

Because state is domain-scoped, repeated updates for one domain do not affect other domains.

---

## Troubleshooting

### “Postman downloaded most files but some suddenly fail”
Election sites often tighten rate limits or change Cloudflare rules mid-run.
Mitigations:
- slow down runner / add delays
- rotate network (different IP)
- try again later
- use probe meta to reduce unnecessary downloads

### “Why is a file getting __dup1?”
Because there is already a file at the canonical destination with **different SHA**.
That implies:
- the website has two distinct versions under same filename
- or you downloaded the same-named file from different contexts

Your system keeps both, but preserves canonical authority.

### “Resort says files are missing”
Resort now prints a “missing files” list: SHA + expected `saved_to`.
This means the index claims the file exists, but it does not exist on disk.
Typical causes:
- manual deletion
- moved outside of tooling
- partial download failures

---

## Quick reference: common commands

Start sink:
```bash
npm start
```

Resort (dry-run):
```bash
node src/index.js resort-downloads --domain=electionresults.govt.nz
```

Resort (apply):
```bash
node src/index.js resort-downloads --domain=electionresults.govt.nz --apply
```

---

## Notes on “JS-injected” file URLs
Some pages may not expose XLS/PDF links in static HTML; links are assembled by JS.
In these cases:
- you may manually add the file URL to your runner list
- or rely on other pages that link it directly
- meta probe can still work if the server responds to HEAD or GET range.

The system treats these URLs like any other; routing + index authority still applies.

---

## License / attribution
This project is focused on tooling and reproducible archival of election results.
Add your license text here as needed.