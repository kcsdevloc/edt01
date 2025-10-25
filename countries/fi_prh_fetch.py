#!/usr/bin/env python3
import argparse, io, json, os, tarfile, time, urllib.parse, urllib.request

FI_OUT_DEFAULT = r"h:\EU_Data\FI"
BASE = "https://avoindata.prh.fi/opendata-xbrl-api/v3"

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def http_get(url, headers=None, timeout=180):
    req = urllib.request.Request(url, headers=headers or {"User-Agent":"Mozilla/5.0","Accept":"application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def _clean_date(d):
    # PRH requires YYYY-MM-DD; slice anything longer (e.g., timestamps) to 10 chars
    return (d or "")[:10]


def main():
    ap = argparse.ArgumentParser(description="Finland PRH XBRL fetcher")
    ap.add_argument("--action", choices=["list","download"], required=True)
    ap.add_argument("--financial-date", default="2023-12-31")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--out", default=FI_OUT_DEFAULT)
    args = ap.parse_args()
    ensure_dir(args.out)

    list_url = f"{BASE}/all_financials?"+urllib.parse.urlencode({"financialDate":args.financial_date})
    lbytes = http_get(list_url)
    if args.action == "list":
        tar_path = os.path.join(args.out, f"FI_list_{args.financial_date}_{time.strftime('%Y%m%d')}.tar.gz")
        with tarfile.open(tar_path,"w:gz") as tf:
            ti = tarfile.TarInfo(f"fi_{args.financial_date}_list.json")
            ti.size = len(lbytes)
            tf.addfile(ti, io.BytesIO(lbytes))
        print("OK list ->", tar_path)
        return

    # download
    data = json.loads(lbytes)
    items = (data.get("financials") or [])[:args.limit]
    tar_path = os.path.join(args.out, f"FI_{args.financial_date}_{len(items)}_{time.strftime('%Y%m%d')}.tar.gz")
    errors = []
    ok = 0
    with tarfile.open(tar_path, "w:gz") as tf:
        for it in items:
            bid = it.get("businessId")
            fd  = _clean_date(it.get("financialDate"))
            if not bid or not fd:
                continue
            try:
                u = f"{BASE}/financial?" + urllib.parse.urlencode({"businessId": bid, "financialDate": fd})
                # API returns Content-Type: text/xml; ask for that
                x = http_get(u, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/xml"})
                name = f"{bid}_{fd}.xbrl"
                ti = tarfile.TarInfo(name); ti.size = len(x)
                tf.addfile(ti, io.BytesIO(x))
                ok += 1
            except Exception as e:
                errors.append(f"{bid},{fd} -> {e}")

        if errors:
            msg = ("\n".join(errors)).encode("utf-8", "replace")
            ti = tarfile.TarInfo("errors.txt"); ti.size = len(msg)
            tf.addfile(ti, io.BytesIO(msg))

    print(f"OK download -> {tar_path} (ok={ok}, err={len(errors)})")


if __name__ == "__main__":
    main()
