# Incremental GitHub CSV Uploader

## Folder structure

```
project/
├── source/          # your original CSVs (1.csv, 2.csv, ...) - never modified
├── upload/           # auto-generated - the incrementally growing upload files
├── state.json         # progress + completion + run log, created on first run
└── uploader.py         # the script - all settings live at the top of the file
```

`project/` must live inside (or under) a git repository that's already
cloned and has a working remote (`git remote -v` should show it, and
`git push` should already work without prompting for credentials).

## Setup

1. Put `uploader.py` in the root of your git repo (or any folder inside it).
2. Put your CSVs in `source/` next to it. Each CSV must have a header row.
3. Add these two lines to `.gitignore` (recommended, not required):
   ```
   state.json
   uploader.lock
   ```
4. Run it once by hand to make sure it works:
   ```
   python3 uploader.py
   ```
5. Schedule it to run daily (the script itself has no scheduler built in —
   use your OS's):
   - **Linux/Mac (cron):** `crontab -e`, then add
     `59 11 * * * cd /path/to/project && python3 uploader.py >> run.log 2>&1`
   - **Mac (launchd):** create a `.plist` with `StartCalendarInterval` set to
     11:59 and `ProgramArguments` pointing at `python3 uploader.py`.
   - **Windows (Task Scheduler):** create a daily trigger at 11:59 that runs
     `python.exe uploader.py` with "Start in" set to the project folder.

## What it does each run

- Picks a random number of upload operations for the day (0–8 by default,
  configurable), remembered in `state.json` so an interrupted day resumes
  where it left off rather than re-rolling the dice.
- For each operation: picks the next unfinished CSV, reads the next slice
  of rows (10–200, streamed — never loads a whole file into memory),
  appends them to `upload/<name>_upload.csv`, commits, and pushes.
- Only advances progress in `state.json` **after** a successful push.

## Robustness features (tested)

- **Crash-safe at any point** — power loss, network loss, GitHub outage, or
  a PC restart mid-run leaves things in a state the next run can always
  recover from cleanly.
- **Push failure → automatic rollback.** If commit succeeds but push
  doesn't (after retries, with a re-sync in between), the local commit and
  the upload-file change are undone with `git reset --hard`, and progress
  is left untouched — so nothing is half-applied.
- **Dirty working tree on startup → auto-repaired.** If a previous run died
  after writing a file but before committing it, the next run detects the
  leftover uncommitted change and discards it (state.json is always the
  source of truth) before doing anything else.
- **Partial-write repair.** If `upload/x_upload.csv` somehow has more rows
  than `state.json` says were uploaded (e.g. write completed but commit
  didn't), it's truncated back in line automatically.
- **Source immutability check.** A SHA-256 hash of each source file is
  recorded the first time it's touched; if the file changes afterward
  (rows added/removed/reordered), that file is skipped with a clear error
  instead of silently uploading the wrong rows.
- **Malformed CSV rows, empty files, missing files, and permission
  errors** are all caught per-file — one bad file gets skipped and logged,
  the rest of the run continues.
- **Header written only once**, on first creation of each upload file.
- **Atomic writes everywhere** — `state.json` and the upload CSVs are
  written to a temp file and swapped in with `os.replace`, so a crash
  mid-write never leaves a corrupted file.
- **Lock file** prevents two runs from overlapping if one run is still in
  progress when cron fires again; a stale lock (>6h old, meaning the prior
  run almost certainly crashed) is detected and overridden automatically.
- **Per-file daily cap** (default: 3) avoids one file eating the whole
  day's iteration budget and producing unnatural-looking commit bursts.
- **`UPLOADER_DRY_RUN=1`** environment variable runs the full selection/
  logging flow without touching git or files — useful for testing config
  changes safely.

## Testing note

This script was exercised against a local throwaway git repo covering:
normal multi-day runs, same-day reruns respecting the iteration budget, an
interrupted write leaving a partial upload file, a fully unreachable
remote, a remote that accepts fetch but rejects push, a mutated source
file, a 0-byte source file, and a malformed CSV row — all recovered or
failed gracefully as described above.
