"""Local fallback: download the Cricsheet zips on your laptop.

Use this only if Databricks serverless compute cannot reach cricsheet.org.
Then upload the zips in data/ to /Volumes/workspace/bronze/raw_files/zips/
through Catalog Explorer and re-run the week 1 notebook.

Usage:  python scripts/download_cricsheet.py
"""
import zipfile
from pathlib import Path

import requests

ZIPS = {
    "ipl": "https://cricsheet.org/downloads/ipl_male_json.zip",
    "t20i_men": "https://cricsheet.org/downloads/t20s_male_json.zip",
}
OUT_DIR = Path(__file__).resolve().parent.parent / "data"


def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    for name, url in ZIPS.items():
        target = OUT_DIR / url.rsplit("/", 1)[1]
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        target.write_bytes(resp.content)
        with zipfile.ZipFile(target) as z:
            n = sum(1 for f in z.namelist() if f.endswith(".json"))
        print(f"{name}: {target.name}, {target.stat().st_size / 1e6:.1f} MB, {n} matches")
    print(f"\nUpload the files in {OUT_DIR} to the raw_files volume, then re-run the notebook.")


if __name__ == "__main__":
    main()
