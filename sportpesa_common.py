# sportpesa_common.py
# Shared SportPesa tennis scraping logic used by singles/doubles runners.
# - Imports Utils as a package (Utils/__init__.py must exist)
# - Competitor normalization happens once per match (not per market)
# - Selections reuse competitor mapping or do lookup-only (no new proposals)
# - Markets fetched in chunks with split-and-retry on HTTP 422

from typing import Iterable, List, Dict, Any, Optional, Tuple
from pathlib import Path
import time
import json
import sqlite3

import requests
from requests.exceptions import RequestException, HTTPError

# --- Import normalization tools from the Utils package ---
from Utils.alias_utils import (
    resolve_or_register as resolve_or_register_player,
    resolve_player as lookup_player_only,
)
from Utils.alias_utils_tournaments import (
    resolve_or_register as resolve_or_register_tournament,
)

# --- Config ---
BASE_URL = "https://www.ke.sportpesa.com/api"
SPORT_ID_TENNIS = 5

# Market IDs you want (adjust as needed)
# 382: Match Winner (2-way), 204: 1st Set Winner, 231: 2nd Set Winner
MARKET_IDS: Optional[List[int]] = [382, 204, 231]

# Chunk size for /games/markets (helps avoid 422 and too-long URLs)
MARKET_CHUNK_SIZE = 20

# Default headers (mimic browser/XHR)
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone": "Africa/Nairobi",
    "Referer": "https://www.ke.sportpesa.com/en/sports-betting/tennis-5/",
}


# --- HTTP helper with retry/backoff ---
def _get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 25,
    retries: int = 4,
    backoff: float = 0.7,
) -> Any:
    h = dict(DEFAULT_HEADERS)
    if headers:
        h.update(headers)
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=h, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except RequestException as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
    if isinstance(last_exc, HTTPError):
        raise last_exc
    raise last_exc if last_exc else RuntimeError("HTTP error without exception?")


# --- SportPesa API client ---
class SportPesaClient:
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url.rstrip("/")

    def fetch_highlights(self, sport_id: int = SPORT_ID_TENNIS) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/highlights/{sport_id}"
        data = _get_json(url)
        return data if isinstance(data, list) else []

    def fetch_live(self, sport_id: int = SPORT_ID_TENNIS) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/live/{sport_id}"
        data = _get_json(url)
        return data if isinstance(data, list) else []

    def fetch_markets(
        self,
        game_ids: Iterable[int],
        market_ids: Optional[Iterable[int]] = MARKET_IDS,
    ) -> Dict[str, Any]:
        """
        Returns: dict keyed by str(match_id) => list of markets.
        Uses chunking plus split-and-retry on HTTP 422.
        """
        ids = [int(x) for x in game_ids if x]
        if not ids:
            return {}

        out: Dict[str, Any] = {}

        def fetch_chunk(chunk: List[int]) -> None:
            if not chunk:
                return
            params = {"games": ",".join(str(i) for i in chunk)}
            if market_ids:
                params["markets"] = ",".join(str(m) for m in market_ids)
            url = f"{self.base_url}/games/markets"
            try:
                data = _get_json(url, params=params)
                if isinstance(data, dict):
                    out.update(data)
            except HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status == 422:
                    # Split the chunk and retry each half
                    if len(chunk) == 1:
                        return
                    mid = len(chunk) // 2
                    fetch_chunk(chunk[:mid])
                    fetch_chunk(chunk[mid:])
                else:
                    raise

        for i in range(0, len(ids), MARKET_CHUNK_SIZE):
            fetch_chunk(ids[i : i + MARKET_CHUNK_SIZE])

        return out


# --- Parsing helpers ---
def is_doubles(competitors: List[Dict[str, Any]]) -> bool:
    if not competitors or len(competitors) < 2:
        return False
    a = (competitors[0].get("name") or "")
    b = (competitors[1].get("name") or "")
    return ("/" in a) or ("/" in b)


def split_pair(name: str) -> List[str]:
    parts = [p.strip() for p in (name or "").split("/") if p.strip()]
    return parts if parts else [name or ""]


def to_iso_utc_from_epoch(ts: Optional[int]) -> str:
    if not ts:
        return ""
    import datetime as _dt

    return _dt.datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _norm_key(s: str) -> str:
    import unicodedata

    s = (s or "").strip().lower()
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )


# --- Per-run caches (avoid duplicate resolver calls) ---
_PLAYER_CACHE: Dict[str, Dict[str, Any]] = {}
_TOURN_CACHE: Dict[str, Dict[str, Any]] = {}


# --- Normalization wrappers ---
def norm_tournament(raw_name: str) -> Dict[str, Any]:
    key = _norm_key(raw_name)
    if key in _TOURN_CACHE:
        return _TOURN_CACHE[key]
    res = resolve_or_register_tournament(
        raw_name, source_bookmaker="SportPesa", event_time_utc=""
    )
    out = (
        {
            "Tournament_ID": res["Tournament_ID"],
            "Tournament_Canonical": res["canonical_name"],
            "Tournament_Status": "hit",
        }
        if res.get("status") == "hit"
        else {
            "Tournament_ID": None,
            "Tournament_Canonical": raw_name,
            "Tournament_Status": "pending",
            "Proposal_ID": res.get("proposal_id"),
        }
    )
    _TOURN_CACHE[key] = out
    return out


def norm_player_single(
    raw_name: str, tournament: str, opponent_raw: str, start_iso: str
) -> Dict[str, Any]:
    key = _norm_key(raw_name)
    if key in _PLAYER_CACHE:
        return _PLAYER_CACHE[key]
    res = resolve_or_register_player(
        raw_name,
        source_bookmaker="SportPesa",
        tournament=tournament,
        opponent_raw=opponent_raw,
        event_time_utc=start_iso,
    )
    out = (
        {"Unique_ID": res["Unique_ID"], "Canonical_Name": res["canonical_name"], "Status": "hit"}
        if res.get("status") == "hit"
        else {
            "Unique_ID": None,
            "Canonical_Name": raw_name,
            "Status": "pending",
            "Proposal_ID": res.get("proposal_id"),
        }
    )
    _PLAYER_CACHE[key] = out
    return out


def norm_pair(
    raw_pair: str, tournament: str, opponent_pair: str, start_iso: str
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    parts = split_pair(raw_pair)
    a = parts[0] if len(parts) >= 1 else ""
    b = parts[1] if len(parts) >= 2 else ""
    n1 = norm_player_single(a, tournament, opponent_pair, start_iso)
    n2 = norm_player_single(b, tournament, raw_pair, start_iso)
    return n1, n2


# --- Row assembly for odds ---
def assemble_rows_for_match(
    match: Dict[str, Any], markets: List[Dict[str, Any]], want_doubles: bool
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    match_id = match.get("id")
    comp_raw = (match.get("competition") or {}).get("name", "") or ""
    competitors = match.get("competitors", []) or []

    match_is_doubles = is_doubles(competitors)
    if want_doubles and not match_is_doubles:
        return rows
    if not want_doubles and match_is_doubles:
        return rows

    p1_raw = (competitors[0].get("name") if len(competitors) >= 1 else "") or ""
    p2_raw = (competitors[1].get("name") if len(competitors) >= 2 else "") or ""
    start_iso = to_iso_utc_from_epoch(match.get("dateTimestamp"))

    # Normalize tournament once per match
    tinfo = norm_tournament(comp_raw)

    # Normalize competitors ONCE and build a map for selection resolution
    resolved_map: Dict[str, Dict[str, Any]] = {}

    if match_is_doubles:
        p1a, p1b = norm_pair(p1_raw, comp_raw, p2_raw, start_iso)
        p2a, p2b = norm_pair(p2_raw, comp_raw, p1_raw, start_iso)

        p1_parts = split_pair(p1_raw)
        p2_parts = split_pair(p2_raw)
        pairs = [
            (p1_parts[0] if len(p1_parts) > 0 else "", p1a),
            (p1_parts[1] if len(p1_parts) > 1 else "", p1b),
            (p2_parts[0] if len(p2_parts) > 0 else "", p2a),
            (p2_parts[1] if len(p2_parts) > 1 else "", p2b),
        ]
        for raw, rec in pairs:
            if raw:
                resolved_map[_norm_key(raw)] = {
                    "Canonical_Name": rec["Canonical_Name"],
                    "Unique_IDs": [rec["Unique_ID"]],
                }

        p1_canon = " / ".join([p1a["Canonical_Name"], p1b["Canonical_Name"]]).strip(" /")
        p2_canon = " / ".join([p2a["Canonical_Name"], p2b["Canonical_Name"]]).strip(" /")
        p1_ids = [p1a["Unique_ID"], p1b["Unique_ID"]]
        p2_ids = [p2a["Unique_ID"], p2b["Unique_ID"]]
    else:
        n1 = norm_player_single(p1_raw, comp_raw, p2_raw, start_iso)
        n2 = norm_player_single(p2_raw, comp_raw, p1_raw, start_iso)
        resolved_map[_norm_key(p1_raw)] = {
            "Canonical_Name": n1["Canonical_Name"],
            "Unique_IDs": [n1["Unique_ID"]],
        }
        resolved_map[_norm_key(p2_raw)] = {
            "Canonical_Name": n2["Canonical_Name"],
            "Unique_IDs": [n2["Unique_ID"]],
        }
        p1_canon = n1["Canonical_Name"]
        p2_canon = n2["Canonical_Name"]
        p1_ids = [n1["Unique_ID"]]
        p2_ids = [n2["Unique_ID"]]

    scope = match.get("scope") or match.get("type") or ""

    for m in (markets or []):
        market_name = m.get("name") or ""
        market_id = m.get("id")
        for sel in m.get("selections", []) or []:
            sel_name_raw = sel.get("name") or ""
            odds = None
            try:
                odds = float(sel.get("odds"))
            except Exception:
                pass

            # Selections: first try competitor map; if not found, do lookup-only (no proposals here)
            sel_key = _norm_key(sel_name_raw)
            sel_canon = sel_name_raw
            sel_ids = [None]

            if sel_key in resolved_map:
                sel_canon = resolved_map[sel_key]["Canonical_Name"]
                sel_ids = resolved_map[sel_key]["Unique_IDs"]
            else:
                hit = lookup_player_only(sel_name_raw)
                if hit:
                    sel_canon = hit["canonical_name"]
                    sel_ids = [hit["Unique_ID"]]
                # else: leave raw (do NOT create proposals from selections)

            rows.append(
                {
                    # Match-level
                    "Bookmaker": "SportPesa",
                    "Match_ID": match_id,
                    "Is_Doubles": bool(match_is_doubles),
                    "Tournament_Raw": comp_raw,
                    "Tournament_Canonical": tinfo["Tournament_Canonical"],
                    "Tournament_ID": tinfo["Tournament_ID"],
                    "Tournament_Status": tinfo["Tournament_Status"],
                    "Start_Time_UTC": start_iso,
                    "Scope": scope,  # "highlight" / "live" / ""

                    # Competitors
                    "Competitor1_Raw": p1_raw,
                    "Competitor2_Raw": p2_raw,
                    "Competitor1_Canonical": p1_canon,
                    "Competitor2_Canonical": p2_canon,
                    "Competitor1_IDs": json.dumps(p1_ids, ensure_ascii=False),
                    "Competitor2_IDs": json.dumps(p2_ids, ensure_ascii=False),

                    # Market
                    "Market_ID": market_id,
                    "Market_Name": market_name,

                    # Selection
                    "Selection_Raw": sel_name_raw,
                    "Selection_Canonical": sel_canon,
                    "Selection_Player_IDs": json.dumps(sel_ids, ensure_ascii=False),
                    "Odds": odds,
                }
            )

    return rows


# --- Main scrape orchestrator (used by singles/doubles entry scripts) ---
def scrape_sportpesa_tennis(
    want_doubles: bool, include_highlights: bool = True, include_live: bool = True
) -> List[Dict[str, Any]]:
    # Clear per-run caches
    _PLAYER_CACHE.clear()
    _TOURN_CACHE.clear()

    client = SportPesaClient()

    matches: List[Dict[str, Any]] = []
    if include_highlights:
        try:
            hl = client.fetch_highlights(SPORT_ID_TENNIS)
            for m in hl:
                m["scope"] = "highlight"
            matches.extend(hl)
        except Exception as e:
            print(f"[WARN] fetch_highlights failed: {e}")

    if include_live:
        try:
            lv = client.fetch_live(SPORT_ID_TENNIS)
            for m in lv:
                m["scope"] = "live"
            matches.extend(lv)
        except Exception as e:
            print(f"[WARN] fetch_live failed: {e}")

    # De-duplicate by match ID, prefer live scope
    seen: Dict[Any, Dict[str, Any]] = {}
    for m in matches:
        mid = m.get("id")
        if mid in seen:
            if m.get("scope") == "live":
                seen[mid] = m
        else:
            seen[mid] = m
    uniq_matches = list(seen.values())

    if not uniq_matches:
        return []

    print(f"[INFO] Found {len(uniq_matches)} matches. Fetching markets…")
    match_ids = [m.get("id") for m in uniq_matches if m.get("id")]
    markets_map = client.fetch_markets(match_ids, MARKET_IDS)

    rows: List[Dict[str, Any]] = []
    for m in uniq_matches:
        mid_str = str(m.get("id"))
        markets = markets_map.get(mid_str, [])
        rows.extend(assemble_rows_for_match(m, markets, want_doubles))

    return rows


# --- Save helpers (Excel/SQLite) ---
def save_to_excel(rows: List[Dict[str, Any]], out_xlsx: Path) -> None:
    import pandas as pd

    df = pd.DataFrame(rows)
    if not len(df):
        print(f"[INFO] No rows to save: {out_xlsx.name}")
        return
    try:
        df.to_excel(out_xlsx, index=False, engine="openpyxl")
    except Exception as e:
        print(f"[WARN] Excel save failed ({e}), saving CSV fallback.")
        df.to_csv(out_xlsx.with_suffix(".csv"), index=False, encoding="utf-8")


def save_to_sqlite(rows: List[Dict[str, Any]], sqlite_path: Path, table_name: str) -> None:
    import pandas as pd

    df = pd.DataFrame(rows)
    if not len(df):
        print(f"[INFO] No rows to save into {table_name}")
        return
    conn = sqlite3.connect(sqlite_path)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        try:
            with conn:
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_match ON {table_name}(Match_ID);"
                )
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_market ON {table_name}(Market_ID);"
                )
        except Exception as e:
            print(f"[WARN] Index creation failed: {e}")
    finally:
        conn.close()
