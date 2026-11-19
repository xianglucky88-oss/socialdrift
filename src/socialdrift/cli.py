
"""Command-line entry point: collect, then inspect history from any viewpoint."""
import argparse
import json
import sys

from . import pipeline


def _parser():
    p = argparse.ArgumentParser(
        prog="socialdrift",
        description="Collect the local source and inspect revision history.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--incremental", action="store_true",
                      help="only process evidence newer than the cursor")
    mode.add_argument("--full", action="store_true", help="collect everything")
    p.add_argument("--db", help="database path (defaults to socialdrift.db)")
    p.add_argument("--history", metavar="LOGICAL_ID",
                   help="print the revision chain of one logical document")
    p.add_argument("--audit", metavar="LOGICAL_ID",
                   help="print provenance: revisions plus the receipt journal")
    p.add_argument("--state", metavar="LOGICAL_ID",
                   help="resolved state of one document at --at/--known-at")
    p.add_argument("--at", metavar="TS",
                   help="valid-time viewpoint: what was true then")
    p.add_argument("--known-at", metavar="TS", dest="known_at",
                   help="receipt-time viewpoint: what had been learned by then")
    p.add_argument("--visible", action="store_true",
                   help="list currently visible documents")
    p.add_argument("--entities", action="store_true",
                   help="list known logical documents")
    p.add_argument("--json", action="store_true", help="pretty JSON output")
    return p


def _emit(value, pretty):
    print(json.dumps(value, ensure_ascii=False,
                     indent=2 if pretty else None, sort_keys=pretty))


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    args = _parser().parse_args(argv)
    read_requested = any([args.history, args.audit, args.state, args.visible,
                          args.entities])

    from .config import DB_PATH
    from .store import Store

    if not read_requested:
        mode = "incremental" if args.incremental else "full"
        _emit(pipeline.run(mode=mode, db_path=args.db), args.json)
        return

    con = Store().connect(args.db or DB_PATH)
    try:
        store = Store(con)
        out = {}
        if args.history:
            out["history"] = store.history(args.history, at=args.at,
                                           known_at=args.known_at)
        if args.audit:
            out["audit"] = store.audit(args.audit)
        if args.state:
            out["state"] = store.state_at(args.state, at=args.at,
                                          known_at=args.known_at)
        if args.visible:
            out["visible"] = sorted(store.visible_latest())
        if args.entities:
            out["entities"] = store.entities()
        _emit(out, args.json)
    finally:
        con.close()


if __name__ == "__main__":
    main()
