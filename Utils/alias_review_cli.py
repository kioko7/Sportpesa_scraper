# Utils/alias_review_cli.py
# Review/approve aliases for players & tournaments.
# Works as module (-m Utils.alias_review_cli) or direct script.

import argparse
import json
import sys
from pathlib import Path

# package-aware imports
if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import alias_utils as P
    import alias_utils_tournaments as T
else:
    from . import alias_utils as P
    from . import alias_utils_tournaments as T

def _pp(x): print(json.dumps(x, ensure_ascii=False, indent=2))

def cmd_seed_aliases(args):
    if args.domain == "players":
        if (args.full or "").lower() in ("1","y","yes","true"):
            n = P.seed_all_alias_variants()
            print(f"Seeded players (canonicals + rich variants): {n}")
        else:
            n = P.seed_aliases_from_canonicals()
            print(f"Seeded player canonicals: {n}")
    else:
        n = T.seed_aliases_from_canonicals()
        print(f"Seeded tournament canonicals: {n}")

def cmd_list(args):
    _, unmapped = (P.load_databases() if args.domain=="players" else T.load_databases())
    rows = unmapped.get("proposals", [])
    if not rows:
        print(f"No pending {args.domain}."); return
    print(f"Pending {args.domain}: {len(rows)}")
    for p in rows:
        print(f"- {p.get('proposal_id')} | guess='{p.get('canonical_name_guess','')}' | sightings={p.get('sightings',0)} | aliases={len(p.get('aliases',[]))}")

def cmd_show(args):
    _, unmapped = (P.load_databases() if args.domain=="players" else T.load_databases())
    r = next((x for x in unmapped.get("proposals",[]) if x.get("proposal_id")==args.id), None)
    if not r: print("Not found"); return
    _pp(r)

def cmd_approve_new_player(args):
    res = P.approve_proposal_as_new(args.id, gender=args.gender or "", preferred_hand=args.hand or "Unknown")
    _pp(res); print("Approved NEW player; queue updated.")

def cmd_duplicate_player(args):
    res = P.mark_proposal_as_duplicate(args.id, int(args.target_id), merge_aliases=(args.merge_aliases.lower() in ("1","yes","y","true")))
    _pp(res); print("Marked duplicate; queue updated.")

def cmd_approve_new_tournament(args):
    res = T.approve_proposal_as_new(args.id, level=args.level or "Unknown", country=args.country or "", city=args.city or "", surface=args.surface or "Unknown")
    _pp(res); print("Approved NEW tournament; queue updated.")

def cmd_duplicate_tournament(args):
    res = T.mark_proposal_as_duplicate(args.id, int(args.target_id), merge_aliases=(args.merge_aliases.lower() in ("1","yes","y","true")))
    _pp(res); print("Marked duplicate; queue updated.")

def cmd_show_meta(args):
    root,_ = (P.load_databases() if args.domain=="players" else T.load_databases())
    _pp(root.get("meta", {}))

def cmd_set_next_id(args):
    value = int(args.value)
    if args.domain == "players":
        root,_ = P.load_databases(); root.setdefault("meta", {}); root["meta"]["next_id"] = value; P.save_players(root)
    else:
        root,_ = T.load_databases(); root.setdefault("meta", {}); root["meta"]["next_id"] = value; T.save_tournaments(root)
    print(f"{args.domain}.meta.next_id set to {value}")

def cmd_kv_counts(_):
    db = Path(__file__).resolve().parent / "aliases_kv.sqlite"
    if not db.exists(): print("KV DB not found:", db); return
    import sqlite3
    con = sqlite3.connect(db); cur = con.cursor()
    for t in ("player_aliases","tournament_aliases"):
        try:
            c = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(t, "rows:", c)
        except Exception as e:
            print(t, "error:", e)
    con.close()

def build_parser():
    p = argparse.ArgumentParser(description="Alias review CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("seed-aliases", help="Seed KV")
    sp.add_argument("domain", choices=["players","tournaments"])
    sp.add_argument("--full", default="no", help="players only: yes to seed canonicals + rich variants")
    sp.set_defaults(func=cmd_seed_aliases)

    sp = sub.add_parser("list", help="List pending items")
    sp.add_argument("domain", choices=["players","tournaments"])
    sp.set_defaults(func=cmd_list)

    sp = sub.add_parser("show", help="Show one pending proposal")
    sp.add_argument("domain", choices=["players","tournaments"])
    sp.add_argument("--id", required=True)
    sp.set_defaults(func=cmd_show)

    sp = sub.add_parser("approve-new-player")
    sp.add_argument("--id", required=True)
    sp.add_argument("--gender", default="Unknown")
    sp.add_argument("--hand", default="Unknown")
    sp.set_defaults(func=cmd_approve_new_player)

    sp = sub.add_parser("duplicate-player")
    sp.add_argument("--id", required=True)
    sp.add_argument("--target-id", required=True)
    sp.add_argument("--merge-aliases", default="yes")
    sp.set_defaults(func=cmd_duplicate_player)

    sp = sub.add_parser("approve-new-tournament")
    sp.add_argument("--id", required=True)
    sp.add_argument("--level", default="Unknown")
    sp.add_argument("--country", default="")
    sp.add_argument("--city", default="")
    sp.add_argument("--surface", default="Unknown")
    sp.set_defaults(func=cmd_approve_new_tournament)

    sp = sub.add_parser("duplicate-tournament")
    sp.add_argument("--id", required=True)
    sp.add_argument("--target-id", required=True)
    sp.add_argument("--merge-aliases", default="yes")
    sp.set_defaults(func=cmd_duplicate_tournament)

    sp = sub.add_parser("show-meta")
    sp.add_argument("domain", choices=["players","tournaments"])
    sp.set_defaults(func=cmd_show_meta)

    sp = sub.add_parser("set-next-id")
    sp.add_argument("domain", choices=["players","tournaments"])
    sp.add_argument("--value", required=True)
    sp.set_defaults(func=cmd_set_next_id)

    sub.add_parser("kv-counts").set_defaults(func=cmd_kv_counts)
    return p

def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)

if __name__ == "__main__":
    sys.exit(main())
