#!/usr/bin/env python3
"""
uploader.py - Incremental GitHub CSV Uploader
------------------------------------------------
Run once per invocation (intended to be triggered daily by cron / Task
Scheduler / launchd at 11:59). Each invocation performs a random number
of "upload operations": it takes the next unfinished CSV in source/,
appends the next slice of its rows to the matching file in upload/,
and commits + pushes that change to GitHub.

Design goals: resumable, fault tolerant, deterministic, never duplicates
rows, never loses progress, safe to interrupt at any point (power loss,
network loss, GitHub outage, PC restart).

State (progress, completion flags, per-file source hashes, run log, and
the daily iteration budget) all lives in state.json next to this script.

ASSUMPTIONS
-----------
- Every file in source/ is a CSV with a header row on line 1.
- Source files are treated as immutable once first processed (a SHA-256
  hash is recorded and checked on every run; a mismatch aborts that file).
- git is already configured with working credentials (SSH key / credential
  helper) for the remote this repo is set up to push to.
- This script lives inside (or under) the git repository it should commit to.

Consider adding state.json and uploader.lock to .gitignore if you don't
want the bookkeeping file itself tracked in the repo's history.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

# ============================== CONFIG ======================================

BASE_DIR = Path(__file__).resolve().parent
SOURCE_DIR = BASE_DIR / "source"
UPLOAD_DIR = BASE_DIR / "upload"
STATE_FILE = BASE_DIR / "state.json"
LOCK_FILE = BASE_DIR / "uploader.lock"

MIN_ROWS_PER_UPLOAD = 10
MAX_ROWS_PER_UPLOAD = 200

MIN_DAILY_ITERATIONS = 0
MAX_DAILY_ITERATIONS = 8

# Cap how many times a single file can be touched within one calendar day,
# so a run doesn't dump all 8 iterations into the same file. Set to None
# to disable the cap.
MAX_UPLOADS_PER_FILE_PER_DAY = 3

# "sequential" = always work on the earliest unfinished file first (safest,
# most predictable). "random" / "weighted_random" are also supported.
SELECTION_STRATEGY = "sequential"

GIT_REMOTE = "origin"
GIT_BRANCH: Optional[str] = None  # None = use whatever branch is checked out
GIT_PUSH_RETRIES = 3
GIT_PUSH_RETRY_DELAY_SECONDS = 5

# Keep state.json from growing forever.
MAX_LOG_ENTRIES = 2000

# Set True (or export UPLOADER_DRY_RUN=1) to run the full flow without
# touching git or advancing progress - useful for testing.
DRY_RUN = os.environ.get("UPLOADER_DRY_RUN", "0") == "1"

# Stale lock threshold - if a lock file older than this is found, assume a
# previous run crashed without cleaning up, warn, and proceed anyway.
STALE_LOCK_SECONDS = 6 * 60 * 60  # 6 hours

# =============================================================================


class UploaderError(Exception):
    """Fatal error that should stop the whole run."""


class FileSkipError(Exception):
    """Non-fatal error scoped to a single source file; skip and continue."""


# ------------------------------ small utilities -----------------------------

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def natural_sort_key(name: str):
    """Sorts '2.csv' before '10.csv' when filenames start with numbers."""
    stem = Path(name).stem
    digits = ""
    for ch in stem:
        if ch.isdigit():
            digits += ch
        else:
            break
    return (int(digits) if digits else float("inf"), name)


def atomic_write_json(path: Path, data: dict) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)  # atomic on POSIX and Windows


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ------------------------------- locking -------------------------------------

def acquire_lock() -> None:
    if LOCK_FILE.exists():
        age = time.time() - LOCK_FILE.stat().st_mtime
        if age < STALE_LOCK_SECONDS:
            print(f"[{now_str()}] Another run appears to be in progress "
                  f"(lock is {age:.0f}s old). Exiting.")
            sys.exit(0)
        print(f"[{now_str()}] WARNING: stale lock file ({age:.0f}s old) "
              f"found - a previous run likely crashed. Proceeding anyway.")
    LOCK_FILE.write_text(f"{os.getpid()} {now_str()}\n", encoding="utf-8")


def release_lock() -> None:
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass


# -------------------------------- git helpers --------------------------------

def find_git_root(start: Path) -> Path:
    cur = start
    for _ in range(20):
        if (cur / ".git").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    raise UploaderError(
        f"No .git repository found starting from {start}. "
        f"Initialize/clone the repo first."
    )


def run_git(args: list[str], cwd: Path) -> str:
    result = subprocess.run(
        ["git", *args], cwd=str(cwd), capture_output=True, text=True
    )
    if result.returncode != 0:
        raise UploaderError(
            f"git {' '.join(args)} failed:\n{result.stderr.strip()}"
        )
    return result.stdout.strip()


def git_current_branch(repo_root: Path) -> str:
    if GIT_BRANCH:
        return GIT_BRANCH
    return run_git(["rev-parse", "--abbrev-ref", "HEAD"], repo_root)


def ensure_clean_working_tree(repo_root: Path) -> None:
    """If a previous run crashed after writing an upload file but before
    `git add`/`commit`, the working tree will be dirty (modified and/or
    untracked files). state.json is only ever advanced after a successful
    push, so it's always safe to throw away uncommitted changes here - the
    per-file repair logic (see process_one_upload) will rebuild whatever is
    needed based on state.json once we start processing again."""
    status = run_git(["status", "--porcelain"], repo_root)
    if not status:
        return
    print(f"[{now_str()}] WARNING: working tree was left dirty by a previous "
          f"run - discarding uncommitted changes before continuing:\n{status}")
    run_git(["reset", "--hard", "HEAD"], repo_root)
    try:
        rel_upload = str(UPLOAD_DIR.relative_to(repo_root))
        run_git(["clean", "-fd", "--", rel_upload], repo_root)
    except UploaderError:
        pass  # nothing untracked to clean, or path outside repo - non-fatal


def git_sync(repo_root: Path) -> None:
    """Fetch + rebase onto remote before doing any work, so we start clean."""
    try:
        ensure_clean_working_tree(repo_root)
        run_git(["fetch", GIT_REMOTE], repo_root)
        branch = git_current_branch(repo_root)
        run_git(["pull", "--rebase", GIT_REMOTE, branch], repo_root)
    except UploaderError as e:
        raise UploaderError(f"Could not sync with remote before starting: {e}")


def git_head(repo_root: Path) -> str:
    return run_git(["rev-parse", "HEAD"], repo_root)


def git_commit_file(repo_root: Path, rel_path: str, message: str) -> None:
    run_git(["add", "--", rel_path], repo_root)
    run_git(["commit", "-m", message], repo_root)


def git_push_with_retries(repo_root: Path) -> bool:
    branch = git_current_branch(repo_root)
    for attempt in range(1, GIT_PUSH_RETRIES + 1):
        result = subprocess.run(
            ["git", "push", GIT_REMOTE, branch],
            cwd=str(repo_root), capture_output=True, text=True,
        )
        if result.returncode == 0:
            return True
        print(f"[{now_str()}] push attempt {attempt}/{GIT_PUSH_RETRIES} "
              f"failed: {result.stderr.strip()[:300]}")
        if attempt < GIT_PUSH_RETRIES:
            try:
                run_git(["fetch", GIT_REMOTE], repo_root)
                run_git(["pull", "--rebase", GIT_REMOTE, branch], repo_root)
            except UploaderError as e:
                print(f"[{now_str()}] resync before retry failed: {e}")
            time.sleep(GIT_PUSH_RETRY_DELAY_SECONDS)
    return False


def git_rollback_to(repo_root: Path, commit_hash: str) -> None:
    """Undo a local commit (and its working-tree changes) that failed to push,
    so the repo returns to the exact state prior to this iteration."""
    subprocess.run(
        ["git", "reset", "--hard", commit_hash],
        cwd=str(repo_root), capture_output=True, text=True,
    )


def build_commit_message(filename: str) -> str:
    n = datetime.now()
    rand = random.randint(100000, 999999)
    return f"{filename} | {n:%Y-%m-%d} | {n:%H-%M-%S} | {rand}"


# ------------------------------ state handling --------------------------------

def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"files": {}, "log": [], "daily": {}}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        raise UploaderError(
            f"state.json is unreadable/corrupted ({e}). Fix or restore it "
            f"from a backup before running again - refusing to guess."
        )
    data.setdefault("files", {})
    data.setdefault("log", [])
    data.setdefault("daily", {})
    return data


def save_state(state: dict) -> None:
    if len(state["log"]) > MAX_LOG_ENTRIES:
        state["log"] = state["log"][-MAX_LOG_ENTRIES:]
    if DRY_RUN:
        return
    atomic_write_json(STATE_FILE, state)


def log_event(state: dict, **fields) -> None:
    entry = {"timestamp": now_str(), **fields}
    state["log"].append(entry)
    status = "OK" if fields.get("success") else "FAIL"
    print(f"[{entry['timestamp']}] {status} "
          f"file={fields.get('file')} rows={fields.get('rows')} "
          f"commit={fields.get('commit')} "
          f"{fields.get('detail', '')}".rstrip())


# ------------------------------ CSV helpers -----------------------------------

def read_header(path: Path) -> list[str]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return []


def count_data_rows(path: Path) -> int:
    """Streams the file to count rows, never loading it fully into memory."""
    count = 0
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for _ in reader:
            count += 1
    return count


def read_row_slice(path: Path, start: int, n: int, expected_cols: int) -> list[list[str]]:
    """Streams to `start`, then reads up to `n` rows without loading the
    whole file. Validates column count consistency along the way."""
    rows: list[list[str]] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        for i, row in enumerate(reader):
            if i < start:
                continue
            if len(rows) >= n:
                break
            if len(row) != expected_cols:
                raise FileSkipError(
                    f"row {i} has {len(row)} columns, expected {expected_cols} "
                    f"(malformed CSV) - skipping this file for now."
                )
            rows.append(row)
    return rows


def count_upload_rows(path: Path) -> int:
    if not path.exists():
        return 0
    return count_data_rows(path)


def write_upload_rows(path: Path, header: list[str], keep_existing_rows: list[list[str]],
                       new_rows: list[list[str]]) -> None:
    """Atomically (re)writes the upload file: header + kept rows + new rows.
    Used both for normal appends and for repairing a partially-written file
    left over from an interrupted run."""
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in keep_existing_rows:
            writer.writerow(row)
        for row in new_rows:
            writer.writerow(row)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def read_upload_rows(path: Path, limit: int) -> list[list[str]]:
    """Read up to `limit` data rows from an existing upload file (used only
    for the rare repair path, so a bounded read is fine)."""
    rows: list[list[str]] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for i, row in enumerate(reader):
            if i >= limit:
                break
            rows.append(row)
    return rows


# ------------------------------ validation -------------------------------------

def validate_source_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileSkipError(f"{path.name} no longer exists in source/.")
    if not os.access(path, os.R_OK):
        raise FileSkipError(f"{path.name} is not readable (permissions).")
    header = read_header(path)
    if not header:
        raise FileSkipError(f"{path.name} has no header row.")
    return header


def ensure_upload_dir_writable() -> None:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    if not os.access(UPLOAD_DIR, os.W_OK):
        raise UploaderError(f"upload/ directory is not writable: {UPLOAD_DIR}")


# ------------------------------ file/day bookkeeping ----------------------------

def ensure_file_state(state: dict, filename: str, path: Path) -> dict:
    entry = state["files"].get(filename)

    # A genuinely 0-byte file (no header, no rows) has nothing to ever
    # upload - mark it completed once and stop looking at it, rather than
    # treating it as a recoverable error that gets retried every run.
    if entry is None and path.exists() and path.stat().st_size == 0:
        entry = {
            "total_rows": 0,
            "uploaded_rows": 0,
            "completed": True,
            "source_hash": sha256_of_file(path),
            "columns": 0,
        }
        state["files"][filename] = entry
        log_event(state, file=filename, rows=0, commit=None, success=True,
                  detail="0-byte file - marked completed immediately.")
        return entry

    header = validate_source_file(path)

    if entry is None:
        file_hash = sha256_of_file(path)
        total_rows = count_data_rows(path)
        entry = {
            "total_rows": total_rows,
            "uploaded_rows": 0,
            "completed": total_rows == 0,
            "source_hash": file_hash,
            "columns": len(header),
        }
        state["files"][filename] = entry
        if total_rows == 0:
            log_event(state, file=filename, rows=0, commit=None, success=True,
                      detail="0 data rows - marked completed immediately.")
    else:
        current_hash = sha256_of_file(path)
        if current_hash != entry["source_hash"]:
            raise FileSkipError(
                f"{filename} changed since it was first processed "
                f"(hash mismatch). Source files must stay immutable once "
                f"upload has started. Skipping to avoid corrupt offsets."
            )
    return entry


def get_daily_budget(state: dict) -> dict:
    today = date.today().isoformat()
    daily = state.get("daily") or {}
    if daily.get("date") != today:
        target = random.randint(MIN_DAILY_ITERATIONS, MAX_DAILY_ITERATIONS)
        daily = {
            "date": today,
            "iterations_target": target,
            "iterations_done": 0,
            "per_file_counts": {},
        }
        state["daily"] = daily
        print(f"[{now_str()}] New day - planned {target} upload "
              f"operation(s) for {today}.")
    return daily


def pick_next_file(state: dict, daily: dict) -> Optional[tuple[str, Path]]:
    candidates = []
    for csv_path in sorted(SOURCE_DIR.glob("*.csv"), key=lambda p: natural_sort_key(p.name)):
        filename = csv_path.name
        entry = state["files"].get(filename)
        if entry and entry.get("completed"):
            continue
        if MAX_UPLOADS_PER_FILE_PER_DAY is not None:
            used_today = daily["per_file_counts"].get(filename, 0)
            if used_today >= MAX_UPLOADS_PER_FILE_PER_DAY:
                continue
        candidates.append((filename, csv_path))

    if not candidates:
        return None

    if SELECTION_STRATEGY == "sequential":
        return candidates[0]
    elif SELECTION_STRATEGY == "random":
        return random.choice(candidates)
    elif SELECTION_STRATEGY == "weighted_random":
        # Weight towards files with the most remaining rows.
        weights = []
        for filename, _ in candidates:
            entry = state["files"].get(filename)
            remaining = 1
            if entry:
                remaining = max(1, entry["total_rows"] - entry["uploaded_rows"])
            weights.append(remaining)
        return random.choices(candidates, weights=weights, k=1)[0]
    else:
        raise UploaderError(f"Unknown SELECTION_STRATEGY: {SELECTION_STRATEGY}")


# --------------------------------- core step ------------------------------------

def process_one_upload(state: dict, repo_root: Path, daily: dict) -> str:
    """Performs a single upload iteration. Returns one of:
      "worked"      - iteration consumed (success or recoverable file skip)
      "no_files"    - nothing eligible left to process at all
      "push_failed" - push failed after retries and was rolled back; the
                      caller should stop the whole run rather than hammer
                      what is likely a broader connectivity/auth problem
    """

    choice = pick_next_file(state, daily)
    if choice is None:
        return "no_files"
    filename, source_path = choice

    try:
        entry = ensure_file_state(state, filename, source_path)
    except FileSkipError as e:
        log_event(state, file=filename, rows=0, commit=None, success=False,
                  detail=f"skipped: {e}")
        daily["per_file_counts"][filename] = daily["per_file_counts"].get(filename, 0) + 1
        return "worked"

    if entry["completed"]:
        return "worked"  # nothing to do, but don't loop forever on it this run

    upload_path = UPLOAD_DIR / f"{Path(filename).stem}_upload.csv"
    header = read_header(source_path)
    expected_cols = entry["columns"]

    # --- repair path: detect a partially-written upload file left over
    # from an interrupted run and bring it back in line with state.json.
    actual_upload_rows = count_upload_rows(upload_path)
    if actual_upload_rows > entry["uploaded_rows"]:
        print(f"[{now_str()}] {filename}: upload file has "
              f"{actual_upload_rows} rows but state says "
              f"{entry['uploaded_rows']} - repairing (interrupted write?).")
        kept = read_upload_rows(upload_path, entry["uploaded_rows"])
        write_upload_rows(upload_path, header, kept, [])
    elif actual_upload_rows < entry["uploaded_rows"]:
        log_event(state, file=filename, rows=0, commit=None, success=False,
                  detail=(f"inconsistent state: upload file only has "
                          f"{actual_upload_rows} rows but state.json says "
                          f"{entry['uploaded_rows']} were uploaded. Needs "
                          f"manual review - skipping this file."))
        daily["per_file_counts"][filename] = daily["per_file_counts"].get(filename, 0) + 1
        return "worked"

    remaining = entry["total_rows"] - entry["uploaded_rows"]
    rows_to_upload = min(random.randint(MIN_ROWS_PER_UPLOAD, MAX_ROWS_PER_UPLOAD), remaining)

    try:
        new_rows = read_row_slice(source_path, entry["uploaded_rows"], rows_to_upload, expected_cols)
    except FileSkipError as e:
        log_event(state, file=filename, rows=0, commit=None, success=False,
                  detail=f"skipped: {e}")
        daily["per_file_counts"][filename] = daily["per_file_counts"].get(filename, 0) + 1
        return "worked"

    if not new_rows:
        return "worked"

    daily["per_file_counts"][filename] = daily["per_file_counts"].get(filename, 0) + 1

    if DRY_RUN:
        log_event(state, file=filename, rows=len(new_rows), commit="DRY-RUN",
                  success=True, detail="(dry run - nothing written or pushed)")
        return "worked"

    # 1. write rows to the upload file (atomic replace, fsynced)
    existing_rows = read_upload_rows(upload_path, entry["uploaded_rows"]) if upload_path.exists() else []
    write_upload_rows(upload_path, header, existing_rows, new_rows)

    rel_path = str(upload_path.relative_to(repo_root))
    rollback_point = git_head(repo_root)

    try:
        # 2. commit
        message = build_commit_message(filename)
        git_commit_file(repo_root, rel_path, message)

        # 3. push (retrying / resyncing on failure)
        if not git_push_with_retries(repo_root):
            raise UploaderError("push failed after all retries")

        commit_hash = git_head(repo_root)

    except UploaderError as e:
        # Push never succeeded (or commit itself failed) - undo any local
        # commit so the repo and the upload file both fall back to the
        # last known-good, pushed state. Progress is NOT advanced.
        git_rollback_to(repo_root, rollback_point)
        log_event(state, file=filename, rows=len(new_rows), commit=None,
                  success=False, detail=f"rolled back: {e}")
        return "push_failed"

    # 4. only now, after a successful push, advance progress
    entry["uploaded_rows"] += len(new_rows)
    if entry["uploaded_rows"] >= entry["total_rows"]:
        entry["completed"] = True

    log_event(state, file=filename, rows=len(new_rows), commit=commit_hash[:7],
              success=True)
    save_state(state)
    return "worked"


# ------------------------------------ main ---------------------------------------

def main() -> int:
    start_time = time.time()
    print(f"[{now_str()}] uploader.py starting"
          f"{' (DRY RUN)' if DRY_RUN else ''}")

    if not SOURCE_DIR.exists():
        print(f"source/ directory not found at {SOURCE_DIR}. Nothing to do.")
        return 1

    ensure_upload_dir_writable()
    acquire_lock()

    try:
        repo_root = find_git_root(BASE_DIR)
        if not DRY_RUN:
            git_sync(repo_root)

        state = load_state()
        daily = get_daily_budget(state)
        save_state(state)

        target = daily["iterations_target"]
        while daily["iterations_done"] < target:
            outcome = process_one_upload(state, repo_root, daily)
            save_state(state)
            if outcome == "no_files":
                print(f"[{now_str()}] No eligible files left to process - "
                      f"stopping early.")
                break
            daily["iterations_done"] += 1
            save_state(state)
            if outcome == "push_failed":
                print(f"[{now_str()}] Stopping the rest of today's run after "
                      f"a push failure - likely a connectivity/auth issue. "
                      f"Remaining iterations will be attempted on the next "
                      f"scheduled run today.")
                break

        save_state(state)

    except UploaderError as e:
        print(f"[{now_str()}] FATAL: {e}")
        return 1
    finally:
        release_lock()

    elapsed = time.time() - start_time
    print(f"[{now_str()}] uploader.py finished in {elapsed:.1f}s "
          f"({daily.get('iterations_done', 0)}/{daily.get('iterations_target', 0)} "
          f"iterations today).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
