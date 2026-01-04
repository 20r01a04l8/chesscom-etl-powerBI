#!/usr/bin/env python3
"""
fetch_and_post.py — Incremental fetcher for Chess.com archives → Google Sheets.

Key guarantees:
- Idempotent by game_url
- Crash-safe state.json (atomic writes)
- Google Sheets API–safe (no illegal resize)
- UTC timezone-correct
"""

from __future__ import annotations

import os
import sys
import time
import json
import base64
import requests
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

import gspread
from google.oauth2 import service_account


# ================= CONFIG =================

DEFAULT_USER_AGENT = "ChessAnalytics/1.0 (+konduvinay47@example.com)"
USER_AGENT = os.environ.get("MAKE_USER_AGENT", DEFAULT_USER_AGENT)
DELAY = float(os.environ.get("CHESS_REQUEST_DELAY", "1.0"))
MAX_RETRIES = int(os.environ.get("CHESS_MAX_RETRIES", "3"))

STATE_FILE = os.environ.get("STATE_FILE", "state.json")
STATE_FILE = os.path.abspath(STATE_FILE)

GSPREAD_SA_JSON_PATH = os.environ.get("GSPREAD_SA_JSON_PATH", "")
SHEET_ID = os.environ.get("SHEET_ID", "")
SHEET_NAME_PREFIX = os.environ.get("SHEET_NAME_PREFIX", "")

GAMES_SHEET = "Games"
PROCESSED_SHEET = "ProceeedArchives"
STATUS_SHEET = "StatusLog"

GAMES_HEADERS = [
    "ingest_time", "username", "archive_url", "game_url", "time_control",
    "end_time_utc", "date_ymd", "white_username", "white_rating",
    "black_username", "black_rating", "result", "pgn"
]

PROCESSED_HEADERS = ["username", "archive_url", "processed_at_utc", "game_count"]
STATUS_HEADERS = ["run_id", "username", "stage", "message", "http_status", "timestamp_utc"]


# ================= UTIL =================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{utc_iso()}] {msg}")


# ================= CHESS API =================

def safe_get_json(url: str) -> Any:
    wait = 2.0
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                raise
            _log(f"[attempt {attempt}] request error: {e}, retrying in {wait}s")
            time.sleep(wait)
            wait *= 2
            continue

        if r.status_code == 200:
            return r.json()

        if r.status_code in (429, 500, 502, 503, 504):
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            _log(f"[attempt {attempt}] HTTP {r.status_code}, retrying in {wait}s")
            time.sleep(wait)
            wait *= 2
            continue

        r.raise_for_status()

    raise RuntimeError(f"Failed GET {url}")


def parse_pgn_result(pgn: str) -> str:
    if not pgn:
        return ""
    m = re.search(r'\[Result\s+"([^"]+)"\]', pgn)
    if m:
        return m.group(1)
    tokens = re.findall(r'\b(1-0|0-1|1/2-1/2)\b', pgn)
    return tokens[-1] if tokens else ""


# ================= GOOGLE SHEETS =================

def _load_service_account_info() -> Dict[str, Any]:
    if GSPREAD_SA_JSON_PATH and os.path.exists(GSPREAD_SA_JSON_PATH):
        with open(GSPREAD_SA_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    b64 = os.environ.get("GSPREAD_SERVICE_ACCOUNT_JSON_B64", "")
    if b64:
        return json.loads(base64.b64decode(b64))

    raise RuntimeError("Service account credentials not found")


def get_gspread_client() -> gspread.Client:
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID is not set")

    sa = _load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(
        sa,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    return gspread.authorize(creds)


def _ensure_headers(ws: gspread.Worksheet, headers: List[str]) -> None:
    existing = ws.row_values(1)
    if existing != headers:
        ws.batch_clear(["A2:Z"])
        ws.update("A1", [headers])


def ensure_sheet_tabs(spreadsheet: gspread.Spreadsheet) -> None:
    tabs = {ws.title: ws for ws in spreadsheet.worksheets()}

    if GAMES_SHEET not in tabs:
        ws = spreadsheet.add_worksheet(GAMES_SHEET, rows=1000, cols=len(GAMES_HEADERS))
        ws.update("A1", [GAMES_HEADERS])
    else:
        _ensure_headers(tabs[GAMES_SHEET], GAMES_HEADERS)

    if PROCESSED_SHEET not in tabs:
        ws = spreadsheet.add_worksheet(PROCESSED_SHEET, rows=1000, cols=len(PROCESSED_HEADERS))
        ws.update("A1", [PROCESSED_HEADERS])
    else:
        _ensure_headers(tabs[PROCESSED_SHEET], PROCESSED_HEADERS)

    if STATUS_SHEET not in tabs:
        ws = spreadsheet.add_worksheet(STATUS_SHEET, rows=1000, cols=len(STATUS_HEADERS))
        ws.update("A1", [STATUS_HEADERS])
    else:
        _ensure_headers(tabs[STATUS_SHEET], STATUS_HEADERS)


def read_existing_game_urls(spreadsheet: gspread.Spreadsheet) -> Set[str]:
    try:
        ws = spreadsheet.worksheet(GAMES_SHEET)
        col = ws.col_values(4)[1:]
        return {c.strip() for c in col if c.strip()}
    except Exception:
        return set()


# ================= STATE (FIXED) =================

def load_state() -> Dict[str, Any]:
    _log(f"Loading state from {STATE_FILE}")
    if not os.path.exists(STATE_FILE):
        return {}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        _log(f"[WARN] Invalid state.json, resetting: {e}")
        return {}


def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


# ================= MAIN =================

def fetch_and_write(usernames_csv: str) -> None:
    usernames = [u.strip() for u in usernames_csv.split(",") if u.strip()]
    if not usernames:
        raise SystemExit("No usernames provided")

    client = get_gspread_client()
    spreadsheet = client.open_by_key(SHEET_ID)

    ensure_sheet_tabs(spreadsheet)

    existing_urls = read_existing_game_urls(spreadsheet)
    _log(f"Existing games in sheet: {len(existing_urls)}")

    state = load_state()
    run_id = os.environ.get("GITHUB_RUN_ID", str(int(time.time())))

    for username in usernames:
        user_state = state.get(username, {})
        processed_archives = set(user_state.get("processed_archives", []))
        last_end_time = int(user_state.get("last_end_time", 0))

        _log(f"Processing {username} (archives processed={len(processed_archives)})")

        archives = safe_get_json(
            f"https://api.chess.com/pub/player/{username}/games/archives"
        ).get("archives", [])

        for archive in archives:
            if archive in processed_archives:
                continue

            _log(f"Fetching {archive}")
            time.sleep(DELAY)

            data = safe_get_json(archive)
            games = data.get("games", [])

            rows = []
            for g in sorted(games, key=lambda x: int(x.get("end_time", 0))):
                url = g.get("url", "")
                if url and url in existing_urls:
                    continue

                et = int(g.get("end_time", 0))
                last_end_time = max(last_end_time, et)

                dt = datetime.fromtimestamp(et, tz=timezone.utc) if et else None

                rows.append([
                    utc_iso(),
                    username,
                    archive,
                    url,
                    g.get("time_control", ""),
                    dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else "",
                    dt.strftime("%Y-%m-%d") if dt else "",
                    g.get("white", {}).get("username", ""),
                    g.get("white", {}).get("rating", ""),
                    g.get("black", {}).get("username", ""),
                    g.get("black", {}).get("rating", ""),
                    parse_pgn_result(g.get("pgn", "")),
                    g.get("pgn", "")
                ])

                if url:
                    existing_urls.add(url)

            if rows:
                spreadsheet.worksheet(GAMES_SHEET).append_rows(rows, value_input_option="USER_ENTERED")

            spreadsheet.worksheet(PROCESSED_SHEET).append_row(
                [username, archive, utc_iso(), len(rows)],
                value_input_option="RAW"
            )

            processed_archives.add(archive)

            state[username] = {
                "last_end_time": last_end_time,
                "processed_archives": sorted(processed_archives)
            }

            save_state(state)

    _log("Run complete")


if __name__ == "__main__":
    usernames = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CHESS_USERNAMES", "")
    if not usernames:
        sys.exit("Provide usernames via CLI or CHESS_USERNAMES")
    fetch_and_write(usernames)
