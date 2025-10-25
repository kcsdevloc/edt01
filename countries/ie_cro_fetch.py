#!/usr/bin/env python3
import argparse, json, os, tarfile, time, urllib.parse, urllib.request, io


IE_OUT_DEFAULT = r"h:\EU_Data\IE"
CKAN_BASE = "https://opendata.cro.ie/api/3/action"
RESOURCE_ID_BY_YEAR = {
    "2023": "dd413039-f628-4931-9788-dfc38eaf6b99",
    "2022": "508d4f8a-74a1-40c7-8b86-cdf0d54a4929",
}

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def http_get(url, headers=None, timeout=180):
    req = urllib.request.Request(url, headers=headers or {"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def ckan_resource_url(resource_id):
    u = f"{CKAN_BASE}/resource_show?{urllib.parse.urlencode({'id': resource_id})}"
    data = json.loads(http_get(u))
    if not data.get("success"): raise RuntimeError("CKAN resource_show failed")
    url = data["result"]["url"]
    if not url: raise RuntimeError("No resource URL returned")
    return url

def save_tar_bytes(out_dir, tar_name, members):
    ensure_dir(out_dir)
    tar_path = os.path.join(out_dir, tar_name)
    with tarfile.open(tar_path, "w:gz") as tf:
        for arcname, b in members:
            ti = tarfile.TarInfo(arcname)
            ti.size = len(b)
            tf.addfile(ti, io.BytesIO(b))
    return tar_path

def main():
    ap = argparse.ArgumentParser(description="Ireland CRO fetcher (CKAN)")
    ap.add_argument("--action", choices=["test","fetch"], required=True)
    ap.add_argument("--year", choices=list(RESOURCE_ID_BY_YEAR.keys()), required=True)
    ap.add_argument("--out", default=IE_OUT_DEFAULT)
    args = ap.parse_args()

    ensure_dir(args.out)
    rid = RESOURCE_ID_BY_YEAR[args.year]

    if args.action == "test":
        u = f"{CKAN_BASE}/datastore_search?"+urllib.parse.urlencode({"resource_id":rid,"limit":100})
        b = http_get(u)
        tar_path = os.path.join(args.out, f"IE_{args.year}_sample_{time.strftime('%Y%m%d')}.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tf:
            ti = tarfile.TarInfo(f"cro_{args.year}_sample.json")
            ti.size = len(b)
            tf.addfile(ti, io.BytesIO(b))
        print("OK test ->", tar_path)
        return

    if args.action == "fetch":
        csv_url = ckan_resource_url(rid)  # resolve live URL
        csv_bytes = http_get(csv_url)
        tar_path = os.path.join(args.out, f"IE_{args.year}_{time.strftime('%Y%m%d')}.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tf:
            ti = tarfile.TarInfo(f"financial_statements_{args.year}.csv")
            ti.size = len(csv_bytes)
            tf.addfile(ti, io.BytesIO(csv_bytes))
        print("OK fetch ->", tar_path)

if __name__ == "__main__":
    main()
