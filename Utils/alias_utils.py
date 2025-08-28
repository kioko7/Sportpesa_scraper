# Utils/alias_utils.py
# Players normalization utilities (robust multi-part names)
# - Cleaner for bookmaker noise
# - Deterministic resolver with:
#   * KV lookup (clean + accentless)
#   * Comma-form support (Last, First ...), hyphenated first names
#   * Multi-part surname handling (de, van, von, el, bin, ...)
#   * Extended in-memory index fallback (fast, read-only)
#   * Auto-heal KV on any hit (so next time it's O(1))
# - Single consolidated unmapped queue (JSON + CSV)
# - Seeding helpers for canonicals + variants

from __future__ import annotations
import json, csv, uuid, re, unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# --- package-local imports (Utils as a package) ---
from .alias_kv import init_db, upsert_player_aliases, get_player_id

BASE = Path(__file__).resolve().parent

# DB files
PLAYERS_JSON = BASE / "all_players.json"
UNMAPPED_JSON = BASE / "unmapped_players.json"
UNMAPPED_CSV  = BASE / "unmapped_players_log.csv"

# Decorations/noise we commonly see in bookmaker strings
_CLEAN_PATTERNS = [
    r"\[[A-Z]{1,3}\]",           # [Q], [WC], [LL]
    r"\(\s*[A-Z]{2,3}\s*\)",     # (USA), (ESP)
    r"\(\s*[Ww][Cc]\s*\)",       # (WC)
    r"\(\s*[Qq]\s*\)",           # (Q)
    r"#\s*\d+",                  # #12
    r"\bseed\s*\d+\b",           # seed 3
    r"\bqualifier\b",            # qualifier
    r"\bretired\b",              # retired
    r"\bwalkover\b",             # walkover
    r"[🟢🔴🟡⚪️⭐️🏳️‍🌈🇦-🇿]+",   # flags/emoji
    r"\s{2,}",                   # extra spaces
]
_CLEAN_RE = re.compile("|".join(_CLEAN_PATTERNS), re.UNICODE | re.IGNORECASE)

def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _load_json(path: Path, default):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def _save_json_atomic(path: Path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    # keep a tiny rotating backup
    if path.exists():
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup = path.with_suffix(path.suffix + f".{ts}.bak")
        try:
            import shutil; shutil.copy2(path, backup)
        except Exception:
            pass
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    import os; os.replace(tmp, path)

def _strip_acc(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def _key(s: str) -> str:
    return (s or "").strip().lower()

def _clean_display_name(s: str) -> str:
    s = s or ""
    s = s.replace("\u200b", "")  # zero-width
    s = s.replace("–", "-").replace("—", "-").replace("’", "'").replace("`","'")
    s = _CLEAN_RE.sub(" ", s)
    # keep letters/digits/space/dash/dot/comma/slash/apostrophe
    s = re.sub(r"[^\w\s\-\.,/']", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s{2,}", " ", s).strip(" ,.-/")
    return s

def _append_unmapped_log(row: Dict):
    header = ["timestamp_utc","raw_name","cleaned","source_bookmaker","tournament","opponent_raw","event_time_utc","proposal_id"]
    exists = UNMAPPED_CSV.exists()
    with open(UNMAPPED_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists: w.writeheader()
        w.writerow({k: row.get(k,"") for k in header})

# --- DB I/O ---
def _ensure_players_meta(players: Dict) -> Dict:
    if "data" not in players:
        data = players
        next_id = (max([int(i) for i in data.keys()]) + 1) if data else 1
        players = {"meta": {"db_version": "players-init", "next_id": next_id}, "data": data}
    if "meta" not in players:
        data = players.get("data", {})
        next_id = (max([int(i) for i in data.keys()]) + 1) if data else 1
        players["meta"] = {"db_version": "players-init", "next_id": next_id}
    if "next_id" not in players["meta"]:
        data = players.get("data", {})
        players["meta"]["next_id"] = (max([int(i) for i in data.keys()]) + 1) if data else 1
    return players

def load_databases():
    players = _load_json(PLAYERS_JSON, {})
    players = _ensure_players_meta(players)
    unmapped = _load_json(UNMAPPED_JSON, {"proposals": []})
    init_db()
    return players, unmapped

def save_players(players: Dict):
    _save_json_atomic(PLAYERS_JSON, players)

def save_unmapped(unmapped: Dict):
    _save_json_atomic(UNMAPPED_JSON, unmapped)

# ---- canonical index for auto-heal fallback ----
def _canonical_index(players: Dict) -> Dict[str, int]:
    idx = {}
    for uid, rec in players.get("data", {}).items():
        key = _key(_strip_acc(rec.get("canonical_name","")))
        if key: idx[key] = int(uid)
    return idx

# ---------------- Multi-part surname handling ----------------
_SURNAME_PARTICLES = {
    # Romance / Iberian / French
    "al","el","bin","ibn","ben","de","del","della","delle","dei","degli","di","do","dos","da","das","du","des","la","le","lo",
    # Dutch / Germanic
    "van","von","der","den","ter","ten","vander","vanden","vande","vd","v.d",
    # Spanish surname joiners / saints
    "san","santa","santo","los","las"
}

def _tokenize(s: str) -> List[str]:
    return [t for t in re.split(r"[ \t\-]+", (s or "").strip()) if t]

def _first_cores_from_given(given: str) -> List[str]:
    """
    Likely 'first cores':
      - full first token (if hyphenated, keep it)
      - hyphen halves
      - dotted initials (J.-L.) and compact initials (JL)
    """
    if not given:
        return []
    t0 = given.split()[0]
    cores = {t0}
    if "-" in t0:
        parts = [p for p in t0.split("-") if p]
        if parts:
            cores.update(parts)                              # Jan, Lennard
            cores.add(".".join(p[0] for p in parts) + ".")  # J.-L.
            cores.add("".join(p[0] for p in parts))         # JL
    return list(cores)

def _last_cores_from_surname(surname: str) -> List[str]:
    """
    Likely 'last cores' from multi-token surname:
      - full surname ("Lopez San Martin")
      - last non-particle token alone ("Martin")
      - particles + last non-particle ("San Martin")
      - last two tokens when both non-particles
      (never return a bare particle)
    """
    toks = _tokenize(surname)
    if not toks: return []
    out = {" ".join(toks)}
    # find rightmost non-particle
    ni = None
    for i in range(len(toks)-1, -1, -1):
        if toks[i].lower() not in _SURNAME_PARTICLES:
            ni = i; break
    if ni is None:
        return list(out)
    out.add(toks[ni])  # last non-particle alone
    j = ni
    while j-1 >= 0 and toks[j-1].lower() in _SURNAME_PARTICLES:
        j -= 1
    core_with_particles = " ".join(toks[j:ni+1])
    out.add(core_with_particles)
    if ni-1 >= 0 and toks[ni-1].lower() not in _SURNAME_PARTICLES:
        out.add(" ".join([toks[ni-1], toks[ni]]))
    out = {v for v in out if v.strip().lower() not in _SURNAME_PARTICLES}
    return list(out)

def _flip_last_first_if_any(name: str) -> str:
    """
    If 'Last, First Middle...' -> 'First Middle... Last' (single player case)
    """
    n = _clean_display_name(name)
    if "," not in n:
        return n
    parts = [p.strip() for p in n.split(",", 1)]
    if len(parts) != 2:
        return n
    last = parts[0]
    first = parts[1]
    if not last or not first:
        return n
    first = " ".join(p.strip().rstrip(".") for p in first.split() if p.strip())
    last  = last.strip()
    return f"{first} {last}".strip()

def _kv_hit(name: str) -> Optional[int]:
    """Return Unique_ID from KV for this display name (clean + accentless)."""
    c = _clean_display_name(name)
    k1 = _key(c)
    uid = get_player_id(k1)
    if uid is None:
        k2 = _key(_strip_acc(c))
        uid = get_player_id(k2)
    return uid

# --------- Extended in-memory index (built on demand; read-only) ----------
_EXT_INDEX: Dict[str,int] | None = None

def _build_ext_index(players: Dict) -> Dict[str,int]:
    """
    Map 'first_core last_core' (lower+accentless) -> Unique_ID.
    Covers multi-part names without exploding KV rows.
    """
    idx: Dict[str,int] = {}
    data = players.get("data", players)
    for uid, rec in data.items():
        uid = int(uid)
        first = (rec.get("first_name","") or "").strip()
        last  = (rec.get("last_name","")  or "").strip()
        canon = (rec.get("canonical_name","") or f"{first} {last}").strip()
        if not (first or last or canon): 
            continue
        first_cores = _first_cores_from_given(first or canon)
        last_cores  = _last_cores_from_surname(last or (canon.split()[-1] if canon else ""))

        keys = {canon}
        if first or last:
            keys.add(f"{(first or canon).split()[0]} {(last or canon.split()[-1])}".strip())

        for f in first_cores:
            for l in last_cores:
                keys.add(f"{f} {l}".strip())

        for k in list(keys):
            nk = _key(_strip_acc(_clean_display_name(k)))
            if nk and nk not in idx:
                idx[nk] = uid
    return idx

def _ext_lookup(name: str, players: Dict) -> Optional[int]:
    global _EXT_INDEX
    if _EXT_INDEX is None:
        _EXT_INDEX = _build_ext_index(players)
    nk = _key(_strip_acc(_clean_display_name(name)))
    return _EXT_INDEX.get(nk)

# --------------------------- UNMAPPED QUEUE ------------------------------
def register_unmapped_player(raw_name: str, source_bookmaker:str="", tournament:str="", opponent_raw:str="", event_time_utc:str="") -> Dict:
    players, unmapped = load_databases()
    cleaned = _clean_display_name(raw_name)
    guess = _parse_name_guess(raw_name)
    aliases = _alias_variants_for(guess["first_name"], guess["last_name"])
    proposal_id = str(uuid.uuid4())
    prop = {
        "proposal_id": proposal_id, "status":"pending",
        "raw_samples":[raw_name],
        "canonical_name_guess": guess["canonical_guess"],
        "first_name_guess": guess["first_name"],
        "last_name_guess": guess["last_name"],
        "gender_guess":"", "preferred_hand":"Unknown",
        "aliases": aliases,
        "created_at_utc": _now_iso(), "last_seen_utc": _now_iso(), "sightings":1,
        "context":[{"source_bookmaker":source_bookmaker,"tournament":tournament,"opponent_raw":opponent_raw,"event_time_utc":event_time_utc}]
    }
    # dedupe within queue by cleaned raw or same canonical guess
    key_clean = _key(_strip_acc(_clean_display_name(raw_name)))
    for p in unmapped.get("proposals", []):
        if _key(_strip_acc(p.get("canonical_name_guess",""))) == _key(_strip_acc(guess["canonical_guess"])) or \
           key_clean in [_key(_strip_acc(_clean_display_name(s))) for s in p.get("raw_samples",[])]:
            p["last_seen_utc"] = _now_iso()
            p["sightings"] = int(p.get("sightings",0)) + 1
            if raw_name not in p["raw_samples"]:
                p["raw_samples"].append(raw_name)
            existing = set(p.get("aliases", []))
            for a in aliases: existing.add(a)
            p["aliases"] = sorted(existing)
            _append_unmapped_log({
                "timestamp_utc": _now_iso(), "raw_name": raw_name, "cleaned": cleaned,
                "source_bookmaker": source_bookmaker, "tournament": tournament,
                "opponent_raw": opponent_raw, "event_time_utc": event_time_utc,
                "proposal_id": p["proposal_id"]
            })
            save_unmapped(unmapped)
            return p

    unmapped.setdefault("proposals", []).append(prop)
    _append_unmapped_log({
        "timestamp_utc": _now_iso(), "raw_name": raw_name, "cleaned": cleaned,
        "source_bookmaker": source_bookmaker, "tournament": tournament,
        "opponent_raw": opponent_raw, "event_time_utc": event_time_utc,
        "proposal_id": proposal_id
    })
    save_unmapped(unmapped)
    return prop

def _parse_name_guess(raw_name: str) -> Dict[str,str]:
    rn = _clean_display_name(raw_name)
    if "," in rn:
        flipped = _flip_last_first_if_any(rn)
        toks = [t for t in flipped.split() if t]
        if len(toks) >= 2:
            first = " ".join(toks[:-1]).title()
            last  = toks[-1].title()
            return {"first_name": first, "last_name": last, "canonical_guess": f"{first} {last}"}
        return {"first_name": flipped.title(), "last_name": "", "canonical_guess": flipped.title()}
    toks = [t for t in rn.replace(".","").split() if t]
    if len(toks) >= 2:
        first = " ".join(toks[:-1]).title()
        last  = toks[-1].title()
        return {"first_name": first, "last_name": last, "canonical_guess": f"{first} {last}"}
    return {"first_name": rn.title(), "last_name": "", "canonical_guess": rn.title()}

# --------------------- RESOLVER (multi-part aware) -----------------------
def resolve_player(raw_name: str) -> Optional[Dict]:
    """
    Deterministic, multi-part aware resolver:
      1) KV with original (clean + accentless)
      2) If comma-form: robust candidates -> KV
         - flip 'Last, First ...' -> 'First ... Last'
         - first-core (first token / hyphen halves / initials) + last-cores
      3) If plain with >2 tokens: derive 'first-core + last-cores' -> KV
      4) Extended in-memory index fallback (no writes) on same candidates
      5) On any hit, auto-heal KV with original & winning candidate
    """
    orig = _clean_display_name(raw_name)

    # 1) KV direct
    uid = _kv_hit(orig)
    flipped = None
    candidates: List[str] = []

    # 2) Comma-form handling (e.g., 'Alcaraz Garfia, Carlos', 'Struff, Jan-Lennard')
    if uid is None and "," in orig:
        last_part, first_part = [p.strip() for p in orig.split(",", 1)]
        flipped = _flip_last_first_if_any(orig)  # 'First ... Last'
        if flipped:
            candidates.append(flipped)

        first_cores = _first_cores_from_given(first_part)
        last_cores  = _last_cores_from_surname(last_part)
        for f in first_cores:
            for l in last_cores:
                cand = f"{f} {l}".strip()
                candidates.append(cand)

        # KV probe for candidates
        for cand in candidates:
            uid = _kv_hit(cand)
            if uid is not None:
                # auto-heal
                pairs = []
                for v in {orig, _strip_acc(orig), cand, _strip_acc(cand)}:
                    k = _key(v)
                    if k:
                        pairs.append((k, int(uid)))
                if pairs:
                    upsert_player_aliases(pairs)
                break

    # 3) Plain multi-token handling: 'Carlos Alcaraz Garfia', 'Botic van de Zandschulp'
    if uid is None and "," not in orig:
        toks = orig.split()
        if len(toks) >= 3:
            # given & surname tail with particles
            given = " ".join(toks[:-1])
            surname = toks[-1]
            j = len(toks)-1
            while j-1 >= 0 and toks[j-1].lower() in _SURNAME_PARTICLES:
                surname = toks[j-1] + " " + surname
                j -= 1
            first_cores = _first_cores_from_given(given)
            last_cores  = _last_cores_from_surname(surname)
            # also consider entire tail as last (e.g., 'Lopez San Martin')
            if len(toks) > 2:
                tail = " ".join(toks[1:])
                last_cores.extend(_last_cores_from_surname(tail))
            seen = set()
            for f in first_cores:
                for l in last_cores:
                    cand = f"{f} {l}".strip()
                    if cand in seen: 
                        continue
                    seen.add(cand)
                    uid = _kv_hit(cand)
                    if uid is not None:
                        # auto-heal
                        pairs = []
                        for v in {orig, _strip_acc(orig), cand, _strip_acc(cand)}:
                            k = _key(v)
                            if k:
                                pairs.append((k, int(uid)))
                        if pairs:
                            upsert_player_aliases(pairs)
                        break
                if uid is not None:
                    break

    # 4) Extended index fallback (read-only)
    if uid is None:
        players, _ = load_databases()
        uid = _ext_lookup(orig, players)
        if uid is None and candidates:
            for cand in candidates:
                uid = _ext_lookup(cand, players)
                if uid is not None:
                    break

    if uid is None:
        return None

    # Build record from all_players.json
    players, _ = load_databases()
    rec = players["data"].get(str(uid), {})
    return {
        "Unique_ID": int(uid),
        "canonical_name": rec.get("canonical_name", ""),
        "first_name": rec.get("first_name", ""),
        "last_name":  rec.get("last_name", ""),
        "gender":     rec.get("gender", ""),
        "preferred_hand": rec.get("preferred_hand", "")
    }

def resolve_players_batch(raw_names: List[str]) -> List[Optional[Dict]]:
    return [resolve_player(n) for n in raw_names]

def resolve_or_register(raw_name: str, **context) -> Dict:
    hit = resolve_player(raw_name)
    if hit:
        return {"status":"hit", **hit}
    prop = register_unmapped_player(raw_name, **context)
    return {"status":"pending", "proposal_id": prop["proposal_id"], "canonical_guess": prop["canonical_name_guess"]}

# --------------------- SEEDING HELPERS (KV) ---------------------
def _alias_variants_for(first: str, last: str) -> List[str]:
    """
    Reasonable set of variants for seeding (bounded; resolver covers the rest):
      - 'First Last', 'Last, First'
      - hyphenated first: halves + initials (J.-L., JL) with last
      - simple truncs (First Las.)
      - accentless + lowercase
    """
    first = (first or "").strip()
    last  = (last or "").strip()
    if not (first or last): return []
    vs = set()
    base_full = f"{first} {last}".strip()
    vs.add(base_full)
    if first and last:
        vs.add(f"{last}, {first}")

    # hyphenated first handling
    if "-" in first:
        parts = [p for p in first.split("-") if p]
        if parts:
            for sub in parts:
                vs.add(f"{sub} {last}")
                vs.add(f"{last}, {sub}")
            init_dots = ".".join(p[0] for p in parts) + "."
            init_comp = "".join(p[0] for p in parts)
            vs.add(f"{init_dots} {last}")
            vs.add(f"{init_comp} {last}")

    # short truncs on last
    if last:
        vs.add(f"{first} {last[:3]}.")
        vs.add(f"{last[:3]}. {first}")

    add = set()
    for v in list(vs):
        na = _strip_acc(v)
        add.add(na)
        add.add(v.lower())
        add.add(na.lower())
    vs.update(add)
    return sorted({re.sub(r"\s{2,}", " ", v).strip() for v in vs if v and v.strip()})

def seed_aliases_from_canonicals() -> int:
    """Seed KV with canonical names (lower + accentless lower)."""
    players, _ = load_databases()
    pairs = []
    for uid, rec in players.get("data", {}).items():
        uid = int(uid)
        canon = rec.get("canonical_name","").strip()
        if not canon:
            first = rec.get("first_name","").strip()
            last  = rec.get("last_name","").strip()
            canon = f"{first} {last}".strip()
        if not canon: 
            continue
        for v in {canon, _strip_acc(canon), canon.lower(), _strip_acc(canon).lower()}:
            k = _key(v)
            if k:
                pairs.append((k, uid))
    if pairs:
        upsert_player_aliases(pairs)
    return len(pairs)

def seed_all_alias_variants() -> int:
    """
    Seed KV with canonical bases PLUS bounded alias variants for ALL players.
    (Resolver handles heavier multi-part logic; we avoid KV explosion.)
    """
    players, _ = load_databases()
    pairs = []
    for uid, rec in players.get("data", {}).items():
        uid = int(uid)
        first = rec.get("first_name","").strip()
        last  = rec.get("last_name","").strip()
        canon = rec.get("canonical_name","").strip() or f"{first} {last}".strip()
        if not (first or last or canon):
            continue
        # base canonicals
        for v in {canon, _strip_acc(canon), canon.lower(), _strip_acc(canon).lower()}:
            k = _key(v)
            if k:
                pairs.append((k, uid))
        # bounded variants
        for v in _alias_variants_for(first, last):
            k = _key(v)
            if k:
                pairs.append((k, uid))
    if pairs:
        upsert_player_aliases(pairs)
    return len(pairs)
