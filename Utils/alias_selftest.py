# Utils/alias_selftest.py
# Progressive, fast, read-only self-test for player alias resolution.
# Usage:
#   python -m Utils.alias_selftest
#   python -m Utils.alias_selftest --start 0 --count 50000 --progress 5000
#   python -m Utils.alias_selftest --out Utils/selftest_player_aliases_failures.csv

from __future__ import annotations
import argparse, csv, time, unicodedata, re
from pathlib import Path
from typing import Dict, List, Tuple

# package-aware import guard
if __package__ is None or __package__ == "":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from alias_utils import load_databases, _clean_display_name, _strip_acc, _key, _flip_last_first_if_any, _SURNAME_PARTICLES
    from alias_kv import get_player_id
else:
    from .alias_utils import load_databases, _clean_display_name, _strip_acc, _key, _flip_last_first_if_any, _SURNAME_PARTICLES
    from .alias_kv import get_player_id

HERE = Path(__file__).resolve().parent

def strip_acc(s: str) -> str:
    return _strip_acc(s)

def gen_variants(first: str, last: str) -> List[str]:
    """
    Realistic bookmaker variants (bounded):
      - 'First Last'
      - 'Last, First'
      - hyphen first: halves + initials with last
      - multi-part last: full last, particles+last, last token only (if not a particle)
      - accentless of all
    """
    first = (first or "").strip()
    last  = (last or "").strip()
    if not (first or last):
        return []
    variants: List[str] = []
    base = f"{first} {last}".strip()
    variants.append(base)
    if last and first:
        variants.append(f"{last}, {first}")

    # hyphenated first handling
    if "-" in first:
        parts = [p for p in first.split("-") if p]
        if parts:
            for sub in parts:
                variants.append(f"{last}, {sub}")
            init_dots = ".".join(p[0] for p in parts) + "."
            init_comp = "".join(p[0] for p in parts)
            variants += [f"{init_dots} {last}", f"{init_comp} {last}"]

    # multi-part last
    toks = [t for t in re.split(r"[ \-]+", last) if t]
    if len(toks) > 1 and first:
        f0 = first.split()[0]
        variants.append(f"{f0} {' '.join(toks)}")
        if toks[-1].lower() not in _SURNAME_PARTICLES:
            variants.append(f"{f0} {toks[-1]}")
        j = len(toks)-1
        while j-1 >= 0 and toks[j-1].lower() in _SURNAME_PARTICLES:
            j -= 1
        core = " ".join(toks[j:])
        variants.append(f"{f0} {core}")

    # accentless + dedupe
    expanded = set()
    for v in variants:
        v = re.sub(r"\s{2,}", " ", v).strip(" ,")
        if not v: continue
        expanded.add(v)
        expanded.add(unicodedata.normalize("NFKD", v).encode("ascii","ignore").decode("ascii"))
    return sorted(expanded)

def kv_hit(name: str) -> int | None:
    # Read-only: check KV for clean + accentless keys
    c = _clean_display_name(name)
    k1 = _key(c)
    uid = get_player_id(k1)
    if uid is None:
        k2 = _key(strip_acc(c))
        uid = get_player_id(k2)
    return uid

def resolve_readonly(raw: str, canon_idx: Dict[str,int]) -> int | None:
    """
    Fast read-only resolve:
      1) KV clean/acc
      2) If comma: try flipped + simple first/last reductions (KV only)
      3) Canonical-index fallback (clean + flipped + reductions)
    """
    c = _clean_display_name(raw)
    # 1) KV direct
    uid = kv_hit(c)
    if uid is not None:
        return uid

    # 2) Comma candidates (KV only)
    candidates: List[str] = []
    if "," in c:
        last_part, first_part = [p.strip() for p in c.split(",", 1)]
        flipped = _flip_last_first_if_any(c)
        if flipped: candidates.append(flipped)
        first_tokens = [t for t in first_part.split() if t]
        last_tokens  = [t for t in last_part.split() if t]
        if first_tokens and last_tokens:
            candidates.append(f"{first_tokens[0]} {' '.join(last_tokens)}".strip())
            if "-" in first_tokens[0]:
                subparts = [p for p in first_tokens[0].split("-") if p]
                for sub in subparts:
                    candidates.append(f"{sub} {' '.join(last_tokens)}".strip())
                init_dots = ".".join(s[0] for s in subparts if s) + "."
                init_comp = "".join(s[0] for s in subparts if s)
                candidates.extend([f"{init_dots} {' '.join(last_tokens)}".strip(),
                                   f"{init_comp} {' '.join(last_tokens)}".strip()])
            if len(last_tokens) > 1:
                for n in range(len(last_tokens)-1, 0, -1):
                    core = " ".join(last_tokens[:n]).strip()
                    candidates.append(f"{first_tokens[0]} {core}")

        for cand in candidates:
            uid = kv_hit(cand)
            if uid is not None:
                return uid

    # 3) Canonical index fallback (read-only)
    uid = canon_idx.get(_key(strip_acc(c)))
    if uid is not None:
        return uid

    if "," in c:
        flipped = _flip_last_first_if_any(c)
        if flipped:
            uid = canon_idx.get(_key(strip_acc(flipped)))
            if uid is not None: return uid

    return None

def main():
    ap = argparse.ArgumentParser(description="Progressive, fast self-test of player resolution.")
    ap.add_argument("--start", type=int, default=0, help="Start index in players list (default 0).")
    ap.add_argument("--count", type=int, default=None, help="Number of players to process (default: all).")
    ap.add_argument("--progress", type=int, default=2000, help="Progress report interval (players).")
    ap.add_argument("--out", default=str(HERE / "selftest_player_aliases_failures.csv"), help="CSV output path for failures.")
    args = ap.parse_args()

    root, _ = load_databases()
    data = root.get("data", root)
    items: List[Tuple[str,Dict]] = list(data.items())
    total_players = len(items)

    # slice sequentially
    start = max(0, args.start)
    end = total_players if args.count is None else min(total_players, start + max(0, args.count))
    work = items[start:end]

    # build canonical index once
    canon_idx: Dict[str,int] = {}
    for uid, rec in data.items():
        canon = (rec.get("canonical_name","") or f"{rec.get('first_name','')} {rec.get('last_name','')}".strip()).strip()
        if not canon: continue
        k = _key(strip_acc(canon))
        if k: canon_idx[k] = int(uid)

    print(f"[INFO] Players total={total_players} | testing {len(work)} from index [{start}:{end})")
    t0 = time.time()

    failures: List[Dict] = []
    tested_players = 0
    checked_variants = 0

    for uid, rec in work:
        first = rec.get("first_name","")
        last  = rec.get("last_name","")
        canon = rec.get("canonical_name", f"{first} {last}".strip())

        variants = gen_variants(first, last)
        if not variants:
            continue

        tested_players += 1
        for v in variants:
            checked_variants += 1
            hit = resolve_readonly(v, canon_idx)
            if hit is None:
                failures.append({"uid": uid, "canonical_name": canon, "variant": v})

        if tested_players % max(1, args.progress) == 0:
            dt = time.time() - t0
            rate = tested_players / dt if dt > 0 else 0.0
            remaining = len(work) - tested_players
            eta = remaining / (rate or 1e-6)
            print(f"[PROGRESS] tested={tested_players}/{len(work)} "
                  f"variants={checked_variants} failures={len(failures)} "
                  f"rate={rate:.1f} players/s ETA={eta/60:.1f} min")

    # write failures CSV
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with open(outp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["uid","canonical_name","variant"])
        w.writeheader()
        w.writerows(failures)

    dt = time.time() - t0
    hit_rate = 0.0 if checked_variants == 0 else (1 - len(failures)/checked_variants) * 100.0
    print("\n===== SUMMARY =====")
    print(f"Players tested:      {tested_players}")
    print(f"Variants checked:    {checked_variants}")
    print(f"Variants missed:     {len(failures)}")
    print(f"Variant hit rate:    {hit_rate:.2f}%")
    print(f"Elapsed:             {dt:.1f}s")
    print("Failures CSV:        ", outp)

if __name__ == "__main__":
    main()
