#!/usr/bin/env python3
import argparse, csv, io, os, tarfile, glob, zipfile, xml.etree.ElementTree as ET

NL_OUT_DEFAULT = r"h:\EU_Data\NL\bulk"

KEYS = [
    "FinancialYear",
    "Assets","AssetsCurrent","AssetsNoncurrent",
    "Equity","EquityAndLiabilities",
    "CashAndCashEquivalents",
    "NetTurnover","ProfitOrLoss","ResultAfterTaxes","OperatingResult"
]
NSK = "{http://schemas.kvk.nl/xb/query/service/2016/1/0/0}key"
NSV = "{http://schemas.kvk.nl/xb/query/service/2016/1/0/0}value"

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def pack_xmls_from_zip(zip_path, out_tar_gz):
    with zipfile.ZipFile(zip_path) as z, tarfile.open(out_tar_gz, "w:gz") as tf:
        for n in z.namelist():
            if not n.lower().endswith(".xml"): continue
            b = z.read(n)
            ti = tarfile.TarInfo(n); ti.size = len(b)
            tf.addfile(ti, io.BytesIO(b))

def bundle_zip_parts(folder, out_tar_gz):
    with tarfile.open(out_tar_gz, "w:gz") as tf:
        for f in sorted(glob.glob(os.path.join(folder, "jaarrekeningen_part*.zip"))):
            tf.add(f, arcname=os.path.basename(f))

def parse_zip_to_csv(zip_path, out_csv, limit):
    with zipfile.ZipFile(zip_path) as z, open(out_csv, "w", newline="", encoding="utf-8") as fw:
        w = csv.DictWriter(fw, fieldnames=["doc","FinancialYear","Assets","AssetsCurrent","AssetsNoncurrent","Equity","EquityAndLiabilities","CashAndCashEquivalents","NetTurnover","ProfitOrLoss","ResultAfterTaxes","OperatingResult"])
        w.writeheader()
        count = 0
        for n in z.namelist():
            if not n.lower().endswith(".xml"): continue
            try:
                root = ET.fromstring(z.read(n))
            except Exception:
                continue
            row = {"doc": os.path.basename(n)}
            for e in root.iter():
                if e.tag.endswith("opendataField"):
                    k = e.attrib.get(NSK) or e.attrib.get("key")
                    v = e.attrib.get(NSV) or e.attrib.get("value")
                    if k in KEYS:
                        row[k] = v
            w.writerow(row)
            count += 1
            if limit and count >= limit: break

def main():
    ap = argparse.ArgumentParser(description="Netherlands KVK HVDS pack/parse")
    ap.add_argument("--action", choices=["pack-xml","bundle-zips","parse-zip"], required=True)
    ap.add_argument("--zip", help="Path to jaarrekeningen_partX.zip")
    ap.add_argument("--folder", default=NL_OUT_DEFAULT, help="Folder with jaarrekeningen_part*.zip")
    ap.add_argument("--out", help="Output file path")
    ap.add_argument("--limit", type=int, default=500, help="For parse-zip: first N XMLs")
    args = ap.parse_args()

    if args.action == "pack-xml":
        if not args.zip or not args.out: raise SystemExit("--zip and --out are required")
        ensure_dir(os.path.dirname(args.out))
        pack_xmls_from_zip(args.zip, args.out)
        print("OK pack-xml ->", args.out)
        return

    if args.action == "bundle-zips":
        if not args.out: raise SystemExit("--out is required")
        ensure_dir(os.path.dirname(args.out))
        bundle_zip_parts(args.folder, args.out)
        print("OK bundle-zips ->", args.out)
        return

    if args.action == "parse-zip":
        if not args.zip or not args.out: raise SystemExit("--zip and --out are required")
        ensure_dir(os.path.dirname(args.out))
        parse_zip_to_csv(args.zip, args.out, args.limit)
        print("OK parse-zip ->", args.out)

if __name__ == "__main__":
    main()
