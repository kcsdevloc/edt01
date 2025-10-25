#!/usr/bin/env python3
import argparse, io, json, os, tarfile, time, urllib.request

FR_OUT_DEFAULT = r"h:\EU_Data\FR"
LOGIN_URL = "https://registre-national-entreprises.inpi.fr/api/sso/login"
ATT_LIST = "https://registre-national-entreprises.inpi.fr/api/companies/{siren}/attachments"
ATT_ZIP  = "https://registre-national-entreprises.inpi.fr/api/companies/{siren}/attachments/download"

def ensure_dir(p): os.makedirs(p, exist_ok=True)
def http_req(url, data=None, headers=None, timeout=180):
    req = urllib.request.Request(url, data=data, headers=headers or {"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def main():
    ap = argparse.ArgumentParser(description="France INPI RNE fetcher")
    ap.add_argument("--action", choices=["login","list-attachments","download-attachments"], required=True)
    ap.add_argument("--user", help="INPI username (email). For login")
    ap.add_argument("--password", help="INPI password. For login")
    ap.add_argument("--token-file", default=r"h:\EU_Data\FR\inpi_token.json")
    ap.add_argument("--siren", help="Company SIREN for attachments")
    ap.add_argument("--out", default=FR_OUT_DEFAULT)
    args = ap.parse_args()
    ensure_dir(args.out)

    if args.action == "login":
        user = args.user or os.environ.get("INPI_USER")
        pw   = args.password or os.environ.get("INPI_PASS")
        if not user or not pw: raise SystemExit("Provide --user and --password or env INPI_USER/INPI_PASS")
        body = json.dumps({"username":user,"password":pw}).encode("utf-8")
        tok = json.loads(http_req(LOGIN_URL, data=body, headers={"Content-Type":"application/json"})).get("token")
        if not tok: raise SystemExit("No token received")
        # ensure parent dir of token-file exists (GH workflow uses out/FR/inpi_token.json)
        parent = os.path.dirname(args.token_file)
        if parent: os.makedirs(parent, exist_ok=True)
        with open(args.token_file, "w", encoding="utf-8") as f:
            json.dump({"token":tok}, f)
        print("OK login ->", args.token_file)
        return


    # below needs token + siren
    if not args.siren: raise SystemExit("--siren required")
    tok = json.load(open(args.token_file, "r", encoding="utf-8"))["token"]
    headers = {"Authorization": f"Bearer {tok}", "User-Agent":"Mozilla/5.0"}

    if args.action == "list-attachments":
        u = ATT_LIST.format(siren=args.siren)
        b = http_req(u, headers=headers)
        outp = os.path.join(args.out, f"attachments_{args.siren}.json")
        with open(outp, "wb") as f: f.write(b)
        print("OK list-attachments ->", outp)
        return

    if args.action == "download-attachments":
        u = ATT_ZIP.format(siren=args.siren)
        z = http_req(u, headers={**headers, "Accept":"application/zip"})
        tar_path = os.path.join(args.out, f"attachments_{args.siren}_{time.strftime('%Y%m%d')}.tar.gz")
        import zipfile
        with zipfile.ZipFile(io.BytesIO(z)) as zf, tarfile.open(tar_path, "w:gz") as tf:
            for n in zf.namelist():
                b = zf.read(n)
                ti = tarfile.TarInfo(n); ti.size = len(b)
                tf.addfile(ti, io.BytesIO(b))
        print("OK download-attachments ->", tar_path)

if __name__ == "__main__":
    main()
