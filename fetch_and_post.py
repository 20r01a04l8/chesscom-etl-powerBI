#!/usr/bin/env python3
"""
fetch_and_post.py (updated)

Sends Chess.com archive games to a Make webhook as an array-of-objects suitable
for Google Sheets -> Bulk Add Rows (Advanced).

Usage:
  MAKE_WEBHOOK (env) must contain the full Make webhook URL.
  MAKE_SECRET (env, optional) contains a secret token to sign the webhook (X-Hook-Token).
  MAKE_SEND_AS_ARRAY_OF_ARRAYS (env, optional) set to "1" to keep legacy array-of-arrays payload.
  Run: python fetch_and_post.py "konduvinay,anotheruser"
"""

from __future__ import annotations
import os
import sys
import time
import json
import requests
from pathlib import Path
from datetime import datetime
from dateutil import tz
from typing import Any, Dict, List

# === Config ===
DEFAULT_USER_AGENT = "ChessAnalytics/1.0 (+your-email@example.com)"
USER_AGENT = os.environ.get("MAKE_USER_AGENT", DEFAULT_USER_AGENT)
DELAY = float(os.environ.get("CHESS_REQUEST_DELAY", "1.0"))  # seconds between chess.com requests
MAX_RETRIES = int(os.environ.get("CHESS_MAX_RETRIES", "3"))
STATE_FILE = os.environ.get("STATE_FILE", "state.json")

MAKE_WEBHOOK = os.environ.get("MAKE_WEBHOOK")
MAKE_SECRET = os.environ.get("MAKE_SECRET")  # optional header token

# Optional flag for backwards-compatible payload (array-of-arrays)
LEGACY_ARRAYS = os.environ.get("MAKE_SEND_AS_ARRAY_OF_ARRAYS", "") == "1"

# Column headers (must match your Google Sheet headers exactly, A..M)
SHEET_COLS = [
    "ingest_time", "username", "archive_url", "game_url", "time_control",
    "end_time_utc", "date_ymd", "white_username", "white_rating",
    "black_username", "black_rating", "result", "pgn"
]

# === Helpers ===
def load_state() -> Dict[str, List[str]]:
    p = Path(STATE_FILE)
    if not p.exists():
        return {}
    try:
        text = p.read_text(encoding="utf-8")
        return json.loads(text or "{}")
    except Exception as e:
        print(f"[WARN] Failed to read {STATE_FILE}: {e} — starting from empty state")
        return {}

def save_state(state: Dict[str, List[str]]) -> None:
    p = Path(STATE_FILE)
    p.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

def safe_get_json(url: str) -> Any:
    """
    GET JSON with polite headers, exponential backoff on 429/5xx, and limited retries.
    """
    wait = 2.0
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as e:
            print(f"[attempt {attempt}] RequestException for {url}: {e}. Sleeping {wait}s")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(wait)
            wait *= 2
            continue

        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                raise RuntimeError(f"Invalid JSON from {url}: {e}")

        if r.status_code in (429, 500, 502, 503, 504):
            print(f"[attempt {attempt}] Retryable status {r.status_code} for {url}. Backoff {wait}s")
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            time.sleep(wait)
            wait *= 2
            continue

        # Non-retryable
        r.raise_for_status()

    raise RuntimeError(f"Failed to GET {url} after {MAX_RETRIES} retries")

def convert_game_to_row(username: str, archive_url: str, game: Dict[str, Any]) -> List[Any]:
    """
    Legacy: convert game to a list (array-of-arrays).
    """
    end_time = game.get("end_time")
    if end_time:
        dt = datetime.utcfromtimestamp(int(end_time))
        end_time_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_ymd = dt.strftime("%Y-%m-%d")
    else:
        end_time_iso = ""
        date_ymd = ""

    row = [
        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),  # ingest_time
        username,
        archive_url,
        game.get("url") or "",
        game.get("time_control") or "",
        end_time_iso,
        date_ymd,
        game.get("white", {}).get("username") or "",
        game.get("white", {}).get("rating") or "",
        game.get("black", {}).get("username") or "",
        game.get("black", {}).get("rating") or "",
        (game.get("white", {}).get("result") or "") + " / " + (game.get("black", {}).get("result") or ""),
        game.get("pgn") or "",
    ]
    return row

def convert_game_to_obj(username: str, archive_url: str, game: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert game to object keyed by SHEET_COLS for Bulk Add Rows (Advanced).
    Keys must match sheet headers exactly.
    """
    row = convert_game_to_row(username, archive_url, game)
    obj = {SHEET_COLS[i]: row[i] for i in range(len(SHEET_COLS))}
    return obj

def post_to_make(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST the payload to Make webhook. If MAKE_WEBHOOK not set -> dry-run print.
    """
    if not MAKE_WEBHOOK:
        print("[DRY RUN] MAKE_WEBHOOK not set. Payload (truncated):")
        print(json.dumps(payload, indent=2, ensure_ascii=False)[:4000])
        return {"dry_run": True}

    headers = {"Content-Type": "application/json", "User-Agent": USER_AGENT}
    if MAKE_SECRET:
        headers["X-Hook-Token"] = MAKE_SECRET

    r = requests.post(MAKE_WEBHOOK, json=payload, headers=headers, timeout=120)
    r.raise_for_status()
    # Try to decode JSON response, otherwise return minimal info
    try:
        return r.json()
    except Exception:
        return {"status": "ok", "http_status": r.status_code, "text": r.text[:200]}

# === Main logic ===
def fetch_and_post(usernames_csv: str) -> None:
    usernames = [u.strip() for u in usernames_csv.split(",") if u.strip()]
    if not usernames:
        raise SystemExit("No usernames provided on the command line.")

    state = load_state()  # dict: {username: [archive_url,...]}

    for username in usernames:
        try:
            print(f"\n=== Checking archives for: {username} ===")
            archives_url = f"https://api.chess.com/pub/player/{username}/games/archives"
            archives_json = safe_get_json(archives_url)
            archives = archives_json.get("archives", []) or []
            processed = set(state.get(username, []))
            new_archives = [a for a in archives if a not in processed]
            print(f"Found {len(archives)} total archives; {len(new_archives)} new")

            for archive in new_archives:
                try:
                    print(f"\nFetching archive: {archive}")
                    time.sleep(DELAY)  # polite delay
                    archive_json = safe_get_json(archive)
                    games = archive_json.get("games", []) or []
                    if not games:
                        print("No games in archive; marking processed")
                        state.setdefault(username, []).append(archive)
                        save_state(state)
                        continue

                    if LEGACY_ARRAYS:
                        # legacy: array-of-arrays (rows as lists)
                        rows_payload = [convert_game_to_row(username, archive, g) for g in games]
                    else:
                        # preferred: array-of-objects for Bulk Add Rows (Advanced)
                        rows_payload = [convert_game_to_obj(username, archive, g) for g in games]

                    payload = {
                        "username": username,
                        "archive_url": archive,
                        "game_count": len(rows_payload),
                        "rows": rows_payload
                    }

                    print(f"Posting payload with {len(rows_payload)} rows to Make webhook (single batch).")
                    resp = post_to_make(payload)
                    print("Make response (truncated):", str(resp)[:400])

                    # mark archive processed and persist
                    state.setdefault(username, []).append(archive)
                    save_state(state)

                except Exception as e:
                    # Log but continue with next archive/username
                    print(f"[ERROR] processing archive {archive}: {e}")
                    # Do not mark processed -> will be retried next run
                    continue

                # polite delay before next archive
                time.sleep(DELAY)

        except Exception as e:
            print(f"[ERROR] checking archives for {username}: {e}")
            continue

    print("\nAll done. State saved to", STATE_FILE)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fetch_and_post.py 'username1,username2'")
        sys.exit(1)
    fetch_and_post(sys.argv[1])
