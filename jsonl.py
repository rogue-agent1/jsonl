#!/usr/bin/env python3
"""jsonl - JSONL (JSON Lines) processor.

One file. Zero deps. Stream JSON.

Usage:
  jsonl.py count file.jsonl              → count lines
  jsonl.py head file.jsonl -n 5          → first 5 records
  jsonl.py tail file.jsonl -n 5          → last 5 records
  jsonl.py filter file.jsonl "status=200" → filter records
  jsonl.py pluck file.jsonl name,email    → extract fields
  jsonl.py stats file.jsonl duration      → numeric stats on field
  jsonl.py to-json file.jsonl             → convert to JSON array
  jsonl.py from-json file.json            → convert from JSON array
  jsonl.py sort file.jsonl timestamp      → sort by field
  jsonl.py uniq file.jsonl id             → deduplicate by field
"""

import argparse
import json
import statistics
import sys


def read_jsonl(path: str):
    f = sys.stdin if path == "-" else open(path)
    try:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    pass
    finally:
        if f is not sys.stdin:
            f.close()


def get_field(obj, key):
    for part in key.split("."):
        if isinstance(obj, dict):
            obj = obj.get(part)
        else:
            return None
    return obj


def matches_filter(obj, expr):
    if "!=" in expr:
        k, v = expr.split("!=", 1)
        return str(get_field(obj, k)) != v
    if ">=" in expr:
        k, v = expr.split(">=", 1)
        val = get_field(obj, k)
        return val is not None and float(val) >= float(v)
    if "<=" in expr:
        k, v = expr.split("<=", 1)
        val = get_field(obj, k)
        return val is not None and float(val) <= float(v)
    if ">" in expr:
        k, v = expr.split(">", 1)
        val = get_field(obj, k)
        return val is not None and float(val) > float(v)
    if "<" in expr:
        k, v = expr.split("<", 1)
        val = get_field(obj, k)
        return val is not None and float(val) < float(v)
    if "=" in expr:
        k, v = expr.split("=", 1)
        return str(get_field(obj, k)) == v
    return True


def cmd_count(args):
    n = sum(1 for _ in read_jsonl(args.file))
    print(n)

def cmd_head(args):
    for i, obj in enumerate(read_jsonl(args.file)):
        if i >= args.n: break
        print(json.dumps(obj, ensure_ascii=False))

def cmd_tail(args):
    buf = []
    for obj in read_jsonl(args.file):
        buf.append(obj)
        if len(buf) > args.n:
            buf.pop(0)
    for obj in buf:
        print(json.dumps(obj, ensure_ascii=False))

def cmd_filter(args):
    for obj in read_jsonl(args.file):
        if all(matches_filter(obj, e) for e in args.expr):
            print(json.dumps(obj, ensure_ascii=False))

def cmd_pluck(args):
    fields = args.fields.split(",")
    for obj in read_jsonl(args.file):
        out = {f: get_field(obj, f) for f in fields}
        print(json.dumps(out, ensure_ascii=False))

def cmd_stats(args):
    values = []
    for obj in read_jsonl(args.file):
        v = get_field(obj, args.field)
        if v is not None:
            try: values.append(float(v))
            except (TypeError, ValueError): pass
    if not values:
        print("No numeric values found")
        return
    print(f"  Count:  {len(values)}")
    print(f"  Min:    {min(values):.4f}")
    print(f"  Max:    {max(values):.4f}")
    print(f"  Mean:   {statistics.mean(values):.4f}")
    print(f"  Median: {statistics.median(values):.4f}")
    if len(values) > 1:
        print(f"  Stdev:  {statistics.stdev(values):.4f}")
    print(f"  Sum:    {sum(values):.4f}")

def cmd_to_json(args):
    records = list(read_jsonl(args.file))
    print(json.dumps(records, indent=2, ensure_ascii=False))

def cmd_from_json(args):
    with open(args.file) as f:
        data = json.load(f)
    if not isinstance(data, list):
        data = [data]
    for obj in data:
        print(json.dumps(obj, ensure_ascii=False))

def cmd_sort(args):
    records = list(read_jsonl(args.file))
    records.sort(key=lambda o: (get_field(o, args.field) is None, get_field(o, args.field)), reverse=args.reverse)
    for obj in records:
        print(json.dumps(obj, ensure_ascii=False))

def cmd_uniq(args):
    seen = set()
    for obj in read_jsonl(args.file):
        key = str(get_field(obj, args.field))
        if key not in seen:
            seen.add(key)
            print(json.dumps(obj, ensure_ascii=False))

def main():
    p = argparse.ArgumentParser(description="JSONL processor")
    sub = p.add_subparsers(dest="cmd")

    for name in ("count",):
        s = sub.add_parser(name); s.add_argument("file")

    for name in ("head", "tail"):
        s = sub.add_parser(name); s.add_argument("file"); s.add_argument("-n", type=int, default=10)

    s = sub.add_parser("filter"); s.add_argument("file"); s.add_argument("expr", nargs="+")
    s = sub.add_parser("pluck"); s.add_argument("file"); s.add_argument("fields")
    s = sub.add_parser("stats"); s.add_argument("file"); s.add_argument("field")
    s = sub.add_parser("to-json"); s.add_argument("file")
    s = sub.add_parser("from-json"); s.add_argument("file")
    s = sub.add_parser("sort"); s.add_argument("file"); s.add_argument("field"); s.add_argument("-r", "--reverse", action="store_true")
    s = sub.add_parser("uniq"); s.add_argument("file"); s.add_argument("field")

    args = p.parse_args()
    if not args.cmd:
        p.print_help()
        return 1
    cmds = {"count": cmd_count, "head": cmd_head, "tail": cmd_tail, "filter": cmd_filter,
            "pluck": cmd_pluck, "stats": cmd_stats, "to-json": cmd_to_json,
            "from-json": cmd_from_json, "sort": cmd_sort, "uniq": cmd_uniq}
    return cmds[args.cmd](args) or 0

if __name__ == "__main__":
    sys.exit(main())
