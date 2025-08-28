import json, csv, unicodedata, uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List

from .alias_kv import init_db, upsert_tournament_aliases, get_tournament_id


# Store all data next to this file (Windows-friendly)
BASE = Path(__file__).resolve().parent

TOUR_JSON = BASE / "all_tournaments.json"   # {"meta":{"db_version","next_id"},"data":{id:{...}}}
UNMAPPED_JSON = BASE / "unmapped_tournaments.json"
UNMAPPED_CSV  = BASE / "unmapped_tournaments_log.csv"

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def _key(s: str) -> str:
    return (s or "").strip().lower()

def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _load_json(path: Path, default):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def _save_json_atomic(path: Path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    # backup
    if path.exists():
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup = path.with_suffix(path.suffix + f".{ts}.bak")
        import shutil; shutil.copy2(path, backup)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    import os; os.replace(tmp, path)

def _ensure_meta(root: Dict) -> Dict:
    if "data" not in root:
        data=root
        next_id = (max([int(i) for i in data.keys()])+1) if data else 1
        root={"meta":{"db_version":"tournaments-init","next_id": next_id},"data":data}
    if "meta" not in root:
        data=root.get("data",{})
        next_id = (max([int(i) for i in data.keys()])+1) if data else 1
        root["meta"]={"db_version":"tournaments-init","next_id": next_id}
    if "next_id" not in root["meta"]:
        data=root.get("data",{})
        root["meta"]["next_id"]=(max([int(i) for i in data.keys()])+1) if data else 1
    return root

def _append_unmapped_log(row: Dict):
    header=["timestamp_utc","raw_name","source_bookmaker","event_time_utc","proposal_id"]
    exists=UNMAPPED_CSV.exists()
    with open(UNMAPPED_CSV,"a",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f, fieldnames=header)
        if not exists: w.writeheader()
        w.writerow({k:row.get(k,"") for k in header})

def load_databases():
    root=_load_json(TOUR_JSON, {})
    root=_ensure_meta(root)
    unmapped=_load_json(UNMAPPED_JSON, {"proposals":[]})
    init_db()
    return root, unmapped

def save_tournaments(root: Dict):
    _save_json_atomic(TOUR_JSON, root)

def save_unmapped(unmapped: Dict):
    _save_json_atomic(UNMAPPED_JSON, unmapped)

def register_unmapped_tournament(raw_name: str, source_bookmaker:str="", event_time_utc:str="") -> Dict:
    root, unmapped = load_databases()
    cname= (raw_name or "").strip().title()
    aliases=sorted(set([raw_name, raw_name.lower(), cname, _strip_accents(raw_name), _strip_accents(raw_name).lower()]))
    proposal_id=str(uuid.uuid4())
    prop={
        "proposal_id": proposal_id, "status":"pending",
        "raw_samples":[raw_name], "canonical_name_guess": cname, "aliases": aliases,
        "created_at_utc": _now_iso(), "last_seen_utc": _now_iso(), "sightings":1,
        "context":[{"source_bookmaker":source_bookmaker,"event_time_utc":event_time_utc}]
    }
    key=_key(_strip_accents(raw_name))
    for p in unmapped.get("proposals",[]):
        if _key(_strip_accents(p.get("canonical_name_guess","")))==_key(cname) or key in [_key(_strip_accents(s)) for s in p.get("raw_samples",[])]:
            p["last_seen_utc"]=_now_iso(); p["sightings"]=int(p.get("sightings",0))+1
            if raw_name not in p["raw_samples"]: p["raw_samples"].append(raw_name)
            ex=set(p.get("aliases",[])); [ex.add(a) for a in aliases]; p["aliases"]=sorted(ex)
            _append_unmapped_log({"timestamp_utc":_now_iso(),"raw_name":raw_name,"source_bookmaker":source_bookmaker,"event_time_utc":event_time_utc,"proposal_id":p["proposal_id"]})
            save_unmapped(unmapped); return p
    unmapped.setdefault("proposals",[]).append(prop)
    _append_unmapped_log({"timestamp_utc":_now_iso(),"raw_name":raw_name,"source_bookmaker":source_bookmaker,"event_time_utc":event_time_utc,"proposal_id":proposal_id})
    save_unmapped(unmapped); return prop

def _remove_proposal(unmapped: Dict, proposal_id: str) -> Dict:
    newprops=[p for p in unmapped.get("proposals",[]) if p.get("proposal_id")!=proposal_id]
    unmapped["proposals"]=newprops
    return unmapped

def approve_proposal_as_new(proposal_id: str, level:str="Unknown", country:str="", city:str="", surface:str="Unknown") -> Dict:
    root, unmapped = load_databases()
    proposals=unmapped.get("proposals",[])
    target=next((p for p in proposals if p.get("proposal_id")==proposal_id), None)
    if not target: raise ValueError(f"Proposal {proposal_id} not found.")
    new_id=int(root["meta"].get("next_id",1))
    root["meta"]["next_id"]=new_id+1
    root["meta"]["db_version"]=f"tournaments-{datetime.utcnow().strftime('%Y%m%d')}"
    cname=target.get("canonical_name_guess","").strip()
    root["data"][str(new_id)]={"canonical_name":cname,"level":level,"country":country,"city":city,"surface":surface}
    pairs=[]
    for a in target.get("aliases",[]):
        k=_key(a)
        ex=get_tournament_id(k)
        if ex is None or ex==new_id: pairs.append((k,new_id))
        noacc=_key(_strip_accents(a))
        if noacc!=k:
            ex2=get_tournament_id(noacc)
            if ex2 is None or ex2==new_id: pairs.append((noacc,new_id))
    if pairs: upsert_tournament_aliases(pairs)
    save_tournaments(root)
    unmapped=_remove_proposal(unmapped, proposal_id); save_unmapped(unmapped)
    return {"Tournament_ID": new_id, **root["data"][str(new_id)]}

def mark_proposal_as_duplicate(proposal_id: str, existing_tournament_id: int, merge_aliases: bool=True) -> Dict:
    root, unmapped = load_databases()
    proposals=unmapped.get("proposals",[])
    target=next((p for p in proposals if p.get("proposal_id")==proposal_id), None)
    if not target: raise ValueError(f"Proposal {proposal_id} not found.")
    if merge_aliases:
        pairs=[]
        for a in target.get("aliases",[]):
            k=_key(a); ex=get_tournament_id(k)
            if ex is None or ex==int(existing_tournament_id): pairs.append((k,int(existing_tournament_id)))
            noacc=_key(_strip_accents(a))
            if noacc!=k:
                ex2=get_tournament_id(noacc)
                if ex2 is None or ex2==int(existing_tournament_id): pairs.append((noacc,int(existing_tournament_id)))
        if pairs: upsert_tournament_aliases(pairs)
    unmapped=_remove_proposal(unmapped, proposal_id); save_unmapped(unmapped)
    return {"action":"duplicate","existing_tournament_id":int(existing_tournament_id),"merged_aliases":bool(merge_aliases),"alias_count_added": len(target.get("aliases",[])) if merge_aliases else 0}

# --- Canonical seeding & auto-heal ---
def _canonical_index(root: Dict) -> Dict[str, int]:
    idx = {}
    for tid, rec in root.get("data", {}).items():
        key = _key(_strip_accents(rec.get("canonical_name","")))
        if key: idx[key] = int(tid)
    return idx

def seed_aliases_from_canonicals() -> int:
    root, _ = load_databases()
    idx = _canonical_index(root)
    pairs = []
    for key, tid in idx.items():
        pairs.append((key, tid))
        raw = root["data"][str(tid)].get("canonical_name","")
        lower = _key(raw)
        if lower and lower != key:
            pairs.append((lower, tid))
    if pairs: upsert_tournament_aliases(pairs)
    return len(pairs)

def resolve_tournament(raw_name: str) -> Optional[Dict]:
    k1=_key(raw_name); tid=get_tournament_id(k1)
    if tid is None:
        k2=_key(_strip_accents(raw_name)); tid=get_tournament_id(k2)
    if tid is None:
        root,_ = load_databases()
        idx = _canonical_index(root)
        tid = idx.get(_key(_strip_accents(raw_name)))
        if tid is not None:
            pairs=[]
            if k1: pairs.append((k1, tid))
            if k2 and k2!=k1: pairs.append((k2, tid))
            if pairs: upsert_tournament_aliases(pairs)
    if tid is None: return None
    root,_ = load_databases()
    rec=root["data"].get(str(tid),{})
    return {"Tournament_ID": int(tid), "canonical_name": rec.get("canonical_name",""),
            "level": rec.get("level",""), "country": rec.get("country",""),
            "city": rec.get("city",""), "surface": rec.get("surface","")}

def resolve_tournaments_batch(raw_names: List[str]) -> List[Optional[Dict]]:
    return [resolve_tournament(n) for n in raw_names]

def resolve_or_register(raw_name: str, **context) -> Dict:
    hit=resolve_tournament(raw_name)
    if hit: return {"status":"hit", **hit}
    prop=register_unmapped_tournament(raw_name, **context)
    return {"status":"pending","proposal_id": prop["proposal_id"], "canonical_guess": prop["canonical_name_guess"]}
