#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import re
import time
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import requests

TIMEOUT = 60

# Statik sayfa: dosya listeleri HTML içinde gelir
ODE_PRODUCT_DETAIL = "https://ode.rsl.wustl.edu/moon/productDetail.aspx?option=hideResize&product_idgeo={ode_id}"


def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def filename_from_url(url: str) -> str:
    return os.path.basename(urlparse(url).path)


def http_head_ok(url: str) -> bool:
    try:
        r = requests.head(url, timeout=TIMEOUT, allow_redirects=True)
        return r.status_code == 200
    except requests.RequestException:
        return False


def download(url: str, out_path: Path, retries: int = 4) -> None:
    safe_mkdir(out_path.parent)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with requests.get(
                url, stream=True, timeout=TIMEOUT, allow_redirects=True
            ) as r:
                r.raise_for_status()
                tmp = out_path.with_suffix(out_path.suffix + ".part")
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
                tmp.replace(out_path)
            return
        except Exception as e:
            last_err = e
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"Download failed: {url}\nLast error: {last_err}")


def extract_le_links_from_html(html: str):
    """
    Find direct PDS links for NAC LE IMG + XML in the productDetail HTML.
    We specifically search for pds.lroc.im-ldi.com links pointing into .../NAC/ and ending with LE.(IMG|xml).
    """
    pattern = re.compile(
        r"https?://pds\.lroc\.im-ldi\.com/[^\s\"']+/NAC/[^\s\"']+LE\.(?:IMG|img|XML|xml)"
    )
    links = pattern.findall(html)

    le_img = None
    le_xml = None
    for u in links:
        fn = filename_from_url(u).lower()
        if fn.endswith(".img") and le_img is None:
            le_img = u
        if fn.endswith(".xml") and le_xml is None:
            le_xml = u

    return le_img, le_xml


def derive_re_from_le(le_url: str) -> str:
    """
    Replace the LE part in the filename with RE (case-preserving) and keep the same folder.
    """
    base = le_url.rsplit("/", 1)[0]
    fn = filename_from_url(le_url)

    # Handle both ...LE... and ...le...
    if "LE" in fn:
        new_fn = fn.replace("LE", "RE")
    elif "le" in fn:
        new_fn = fn.replace("le", "re")
    else:
        raise ValueError(f"Filename does not contain LE/le: {fn}")

    return f"{base}/{new_fn}"


def run_gdalinfo(in_path: Path, report_path: Path) -> None:
    try:
        proc = subprocess.run(
            ["gdalinfo", str(in_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        safe_mkdir(report_path.parent)
        report_path.write_text(
            proc.stdout + ("\n\n--- STDERR ---\n" + proc.stderr if proc.stderr else ""),
            encoding="utf-8",
        )
    except FileNotFoundError:
        # gdalinfo yoksa rapor üretmeyiz
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", required=True)
    ap.add_argument("--product_idgeo", required=True, type=int)
    ap.add_argument("--out", default=".")
    args = ap.parse_args()

    region_dir = Path(args.out) / args.region
    left_dir = region_dir / "LRO_left"
    right_dir = region_dir / "LRO_right"
    reports_dir = region_dir / "reports"
    safe_mkdir(left_dir)
    safe_mkdir(right_dir)
    safe_mkdir(reports_dir)

    # 1) Fetch static product detail HTML
    page_url = ODE_PRODUCT_DETAIL.format(ode_id=args.product_idgeo)
    try:
        html = requests.get(
            page_url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"}
        ).text
    except requests.RequestException as e:
        raise RuntimeError(f"Could not fetch ODE product detail page:\n{page_url}\n{e}")

    # 2) Extract LE links
    le_img, le_xml = extract_le_links_from_html(html)
    if not le_img or not le_xml:
        raise RuntimeError(
            "LE IMG/XML direct PDS links not found in productDetail HTML.\n"
            f"Page: {page_url}\n"
            "If you can see links in browser but script can't, paste the page HTML to inspect (rare)."
        )

    # 3) Derive RE links
    re_img = derive_re_from_le(le_img)
    re_xml = derive_re_from_le(le_xml)

    # 4) Download
    le_img_path = left_dir / filename_from_url(le_img)
    le_xml_path = left_dir / filename_from_url(le_xml)
    re_img_path = right_dir / filename_from_url(re_img)
    re_xml_path = right_dir / filename_from_url(re_xml)

    download(le_img, le_img_path)
    download(le_xml, le_xml_path)

    # RE doğrulama (HEAD bazen engellenir; yine de GET deniyoruz)
    _ = http_head_ok(re_img)
    download(re_img, re_img_path)
    download(re_xml, re_xml_path)

    # 5) gdalinfo reports (if available)
    run_gdalinfo(le_img_path, reports_dir / "gdalinfo_left_img.txt")
    run_gdalinfo(re_img_path, reports_dir / "gdalinfo_right_img.txt")

    # 6) Manifest
    manifest_txt = (
        f"region={args.region}\n"
        f"product_idgeo={args.product_idgeo}\n\n"
        f"LE_IMG={le_img}\n"
        f"LE_XML={le_xml}\n"
        f"RE_IMG={re_img}\n"
        f"RE_XML={re_xml}\n"
        f"ODE_product_detail={page_url}\n"
    )
    (region_dir / "download_manifest.txt").write_text(manifest_txt, encoding="utf-8")

    print("OK")
    print(f"Saved to: {region_dir}")
    print(f"Left : {le_img_path.name}, {le_xml_path.name}")
    print(f"Right: {re_img_path.name}, {re_xml_path.name}")


if __name__ == "__main__":
    main()

# python download_lroc_stereo_by_product_idgeo.py --region Aristillus_Direct_Data --product_idgeo 38596567 --out .
