import sqlite3
from pathlib import Path
from typing import Iterable, Tuple, Optional, List

# ✅ Place the database file next to this script
DB_PATH = Path(__file__).resolve().parent / "aliases_kv.sqlite"

def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    with _conn() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS player_aliases (
            alias TEXT PRIMARY KEY,
            unique_id INTEGER NOT NULL
        );
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS tournament_aliases (
            alias TEXT PRIMARY KEY,
            tournament_id INTEGER NOT NULL
        );
        """)

def upsert_player_aliases(pairs: Iterable[Tuple[str,int]]):
    pairs = list(pairs)
    if not pairs: return
    with _conn() as con:
        con.executemany(
            "INSERT INTO player_aliases(alias, unique_id) VALUES(?, ?) "
            "ON CONFLICT(alias) DO UPDATE SET unique_id=excluded.unique_id;",
            pairs
        )

def upsert_tournament_aliases(pairs: Iterable[Tuple[str,int]]):
    pairs = list(pairs)
    if not pairs: return
    with _conn() as con:
        con.executemany(
            "INSERT INTO tournament_aliases(alias, tournament_id) VALUES(?, ?) "
            "ON CONFLICT(alias) DO UPDATE SET tournament_id=excluded.tournament_id;",
            pairs
        )

def get_player_id(alias: str) -> Optional[int]:
    with _conn() as con:
        cur = con.execute("SELECT unique_id FROM player_aliases WHERE alias=?;", (alias,))
        row = cur.fetchone()
        return row[0] if row else None

def get_tournament_id(alias: str) -> Optional[int]:
    with _conn() as con:
        cur = con.execute("SELECT tournament_id FROM tournament_aliases WHERE alias=?;", (alias,))
        row = cur.fetchone()
        return row[0] if row else None

def export_player_aliases() -> List[Tuple[str,int]]:
    with _conn() as con:
        return list(con.execute("SELECT alias, unique_id FROM player_aliases"))

def export_tournament_aliases() -> List[Tuple[str,int]]:
    with _conn() as con:
        return list(con.execute("SELECT alias, tournament_id FROM tournament_aliases"))
