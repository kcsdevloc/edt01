#!/usr/bin/env python3
import argparse, csv, os

FIELDS = ["country","source","company_id","company_name","year","revenue","profit","assets","doc_id","raw_year","raw_revenue","raw_profit","raw_assets"]

def main():
    ap = argparse.ArgumentParser(description="Union multiple normalized CSVs")
    ap.add_argument("--out", required=True, help="Output CSV")
    ap.add_argument("inputs", nargs="+", help="Input CSV files")
    args = ap.parse_args()

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    total = 0
    with open(args.out, "w", newline="", encoding="utf-8") as fw:
        w = csv.DictWriter(fw, fieldnames=FIELDS)
        w.writeheader()
        for path in args.inputs:
            if not os.path.isfile(path):
                print("Skip missing", path); continue
            with open(path, "r", encoding="utf-8") as fr:
                r = csv.DictReader(fr)
                for row in r:
                    out = {k: row.get(k) for k in FIELDS}
                    w.writerow(out)
                    total += 1
    print("OK rows", total, "->", args.out)

if __name__ == "__main__":
    main()
