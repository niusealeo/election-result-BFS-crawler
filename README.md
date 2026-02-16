# sink (modular)

This is the modular refactor of `sink.js`.

## Run
```bash
npm install
npm start
```

## Env
- `PORT` (default 3000)
- `ARTIFACT_META_FIRST_ROW` (default 1). Set to `0` to disable the conflated `_meta` first row format.

## On-disk layout (created relative to process cwd)
- `./BFS_crawl/`
  - `runs/` (jsonl logs)
  - `_meta/`
    - `state.json`
    - `electorates_by_term.json`
    - `artifacts/`
      - `urls-level-<L>.json`
      - `files-level-<L>.json`
- `./downloads/` (downloaded files, routed by term/electorate)
